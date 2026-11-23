#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["markdown-it-py==4.0.0", "playwright==1.56.0"]
# ///
"""Render Markdown tables with local Chromium (uv manages dependencies).

Usage: markdown-table-image input.md output.png --title 'Comparison'
Use - for stdin. Tables split at 12 rows and 4 columns, repeating headers
and the first column. Pages taller than the height limit are split further;
a single row that still overflows fails explicitly. Additional images receive
-2, -3, etc. Dark PNGs use 2x resolution. --manifest emits an ordered JSON array of text/image objects,
retaining surrounding Markdown and clickable table URLs after each table.
Raw HTML is disabled, images become alt text, and network requests are blocked.
Limits: 1 MiB input/title, 64 images, 4096 CSS pixels per image (8192 physical).
Oversized content fails explicitly; nothing is silently truncated.
"""
import argparse
import html
import json
import math
import os
from pathlib import Path
import re
import shutil
import sys
from urllib.parse import urlsplit

from markdown_it import MarkdownIt
import signal

MAX_SOURCE_BYTES = 1024 * 1024
MAX_IMAGES = 64
MAX_HEIGHT = 4096
CSS = """
* { box-sizing: border-box; }
body { margin: 0; background: #202225; color: #f2f3f5;
       font-family: 'DejaVu Sans', sans-serif; }
article { padding: 24px; width: 960px; }
h1 { font-size: 26px; margin: 0 0 18px; overflow-wrap: anywhere; }
table { border-collapse: collapse; width: 100%; table-layout: fixed;
        font-size: 22px; line-height: 1.45; }
th, td { padding: 14px 16px; border: 1px solid #555963;
         vertical-align: top; overflow-wrap: anywhere; }
th { background: #383d48; font-weight: 700; }
td { background: #292c33; }
tr:nth-child(even) td { background: #30343c; }
a { color: #9dcaff; text-decoration: underline; }
code { font-size: 0.92em; color: #f4d9a0; }
footer { margin-top: 14px; font-size: 16px; color: #c0c3ca; }
"""


class RenderError(Exception):
    """Only fixed, source-independent diagnostics may cross this boundary."""


def extract_tables(source):
    if len(source.encode("utf-8")) > MAX_SOURCE_BYTES:
        raise RenderError("Markdown exceeds the 1 MiB input limit")
    parser = MarkdownIt("commonmark", {"html": False}).enable("table")
    parser.renderer.rules["image"] = lambda tokens, idx, options, env: html.escape(tokens[idx].content)
    # Markdown-it normalizes CRLF and CR; retain original byte-for-byte prose.
    offsets = [0, *(match.end() for match in re.finditer(r"\r\n|\r|\n", source))]
    offsets.append(len(source))
    tables = []
    current = None
    row = None
    alignment = "left"
    for token in parser.parse(source):
        if token.type == "table_open":
            start, end = token.map
            current = {"start": offsets[start], "end": offsets[end], "rows": [], "links": []}
        elif token.type == "tr_open" and current is not None:
            row = []
        elif token.type in ("th_open", "td_open"):
            style = token.attrGet("style") or ""
            alignment = "right" if "right" in style else "center" if "center" in style else "left"
        elif token.type == "inline" and current is not None and row is not None:
            children = token.children or []
            row.append((parser.renderer.renderInline(children, parser.options, {}), alignment))
            for child in children:
                url = child.attrGet("href") if child.type == "link_open" else child.attrGet("src") if child.type == "image" else None
                if url and urlsplit(url).scheme.lower() in ("http", "https", "mailto"):
                    if url not in current["links"]:
                        current["links"].append(url)
        elif token.type == "tr_close" and current is not None:
            current["rows"].append(row)
            row = None
        elif token.type == "table_close":
            tables.append(current)
            current = None
    return tables


def column_groups(count, columns_per_image):
    if count <= columns_per_image:
        return [list(range(count))]
    return [[0, *range(start, min(start + columns_per_image - 1, count))]
            for start in range(1, count, columns_per_image - 1)]


def page_count(table, rows_per_image, columns_per_image):
    columns = len(table["rows"][0])
    groups = 1 if columns <= columns_per_image else math.ceil((columns - 1) / (columns_per_image - 1))
    return groups * max(1, math.ceil((len(table["rows"]) - 1) / rows_per_image))


def page_document(table, table_index, title, column_index, total_groups, columns, rows, row_offset, total_rows):
    header, *_ = table["rows"]
    cells = []
    for row_index, row in enumerate([header, *rows]):
        tag = "th" if row_index == 0 else "td"
        cells.append("<tr>" + "".join(
            f'<{tag} style="text-align:{row[c][1]}">{row[c][0]}</{tag}>' for c in columns
        ) + "</tr>")
    label = f"Table {table_index} · Columns {column_index}/{total_groups}"
    if rows:
        label += f" · Rows {row_offset + 1}–{min(row_offset + len(rows), total_rows)}/{total_rows}"
    return ("<!doctype html><meta charset='utf-8'>"
           "<meta http-equiv='Content-Security-Policy' content=\"default-src 'none'; style-src 'unsafe-inline'\">"
           f"<style>{CSS}</style><article><h1>{html.escape(title)}</h1>"
           f"<table>{''.join(cells)}</table><footer>{label}</footer></article>")


def documents(table, table_index, title, rows_per_image, columns_per_image):
    header, *rows = table["rows"]
    groups = column_groups(len(header), columns_per_image)
    for column_index, columns in enumerate(groups, 1):
        for start in range(0, max(len(rows), 1), rows_per_image):
            yield page_document(
                table, table_index, title, column_index, len(groups),
                columns, rows[start:start + rows_per_image], start, len(rows),
            )


def manifest_parts(source, tables, image_paths):
    parts = []
    cursor = 0
    for table, paths in zip(tables, image_paths, strict=True):
        if table["start"] > cursor:
            parts.append({"text": source[cursor:table["start"]]})
        parts.extend({"image": str(path)} for path in paths)
        if table["links"]:
            # Angle brackets preserve clickability without Discord link previews.
            links = [url.replace("<", "%3C").replace(">", "%3E") for url in table["links"]]
            parts.append({"text": "\nTable links:\n" + "\n".join(f"<{url}>" for url in links) + "\n"})
        cursor = table["end"]
    if cursor < len(source):
        parts.append({"text": source[cursor:]})
    return parts


def render(source, output, title, rows_per_image, columns_per_image, manifest):
    if rows_per_image < 1 or columns_per_image < 2:
        raise RenderError("--rows must be positive and --columns must be at least 2")
    if output.suffix.lower() != ".png":
        raise RenderError("output must have a .png extension")
    if len(title.encode("utf-8")) > MAX_SOURCE_BYTES:
        raise RenderError("Title exceeds the 1 MiB limit")
    tables = extract_tables(source)
    if not tables:
        if manifest:
            return manifest_parts(source, [], [])
        raise RenderError("No Markdown tables found")
    if sum(page_count(table, rows_per_image, columns_per_image) for table in tables) > MAX_IMAGES:
        raise RenderError("Table output exceeds the 64 image limit")
    chromium = shutil.which("chromium")
    if not chromium:
        raise RenderError("chromium is required")
    output.parent.mkdir(parents=True, exist_ok=True)
    directory = output.parent.resolve()
    output = directory / output.name
    image_paths = []
    created = []
    # Import lazily: deterministic parser tests do not need a browser runtime.
    from playwright.sync_api import sync_playwright
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(executable_path=chromium, headless=True)
            context = browser.new_context(viewport={"width": 960, "height": 800},
                                          device_scale_factor=2, java_script_enabled=False,
                                          service_workers="block", accept_downloads=False)
            context.route("**/*", lambda route: route.abort())
            page = context.new_page()
            page.set_default_timeout(15000)
            for table_index, table in enumerate(tables, 1):
                header, *rows = table["rows"]
                groups = column_groups(len(header), columns_per_image)
                total_groups = len(groups)
                paths = []

                def emit_page(column_index, columns, row_start, row_end):
                    # Emit images for rows[row_start:row_end], splitting the range
                    # on overflow so no single image exceeds the height limit.
                    document = page_document(
                        table, table_index, title, column_index, total_groups,
                        columns, rows[row_start:row_end], row_start, len(rows),
                    )
                    page.set_content(document, wait_until="load")
                    article = page.locator("article")
                    bounds = article.bounding_box()
                    if bounds and bounds["height"] <= MAX_HEIGHT:
                        if len(created) >= MAX_IMAGES:
                            raise RenderError("Table output exceeds the 64 image limit")
                        index = len(created) + 1
                        path = output if index == 1 else output.with_stem(f"{output.stem}-{index}")
                        if path.is_symlink() or path.resolve().parent != directory:
                            raise RenderError("Output path must stay within the output directory")
                        # Open first without following symlinks; screenshots are bounded above.
                        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
                        created.append(path)
                        with os.fdopen(fd, "wb") as destination:
                            destination.write(article.screenshot())
                        return [path]
                    if row_end - row_start <= 1:
                        raise RenderError("Table image exceeds the 4096 pixel height limit")
                    mid = row_start + (row_end - row_start + 1) // 2
                    return (emit_page(column_index, columns, row_start, mid)
                            + emit_page(column_index, columns, mid, row_end))

                for column_index, columns in enumerate(groups, 1):
                    if not rows:
                        # A header-only table still renders one page.
                        paths.extend(emit_page(column_index, columns, 0, 0))
                    else:
                        start = 0
                        while start < len(rows):
                            end = min(start + rows_per_image, len(rows))
                            paths.extend(emit_page(column_index, columns, start, end))
                            start = end
                image_paths.append(paths)
            browser.close()
    except BaseException:
        for path in created:
            path.unlink(missing_ok=True)
        raise
    return manifest_parts(source, tables, image_paths) if manifest else [str(path) for path in created]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("input", help="Markdown file, or - for stdin")
    ap.add_argument("output", type=Path, help="Output PNG; additional images receive -2, -3, etc.")
    ap.add_argument("--title", default="Table")
    ap.add_argument("--rows", type=int, default=12)
    ap.add_argument("--columns", type=int, default=4)
    ap.add_argument("--manifest", action="store_true")
    args = ap.parse_args()
    # Unwind Playwright on transport cancellation instead of abandoning Chromium.
    def terminate(signum, frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, terminate)
    try:
        if args.input == "-":
            raw = sys.stdin.buffer.read(MAX_SOURCE_BYTES + 1)
        else:
            with Path(args.input).open("rb") as source_file:
                raw = source_file.read(MAX_SOURCE_BYTES + 1)
        if len(raw) > MAX_SOURCE_BYTES:
            raise RenderError("Markdown exceeds the 1 MiB input limit")
        result = render(raw.decode("utf-8"), args.output, args.title, args.rows, args.columns, args.manifest)
        print(json.dumps(result, ensure_ascii=False) if args.manifest else "\n".join(result))
        return 0
    except RenderError as error:
        print(f"markdown-table-image: {error}", file=sys.stderr)
    except Exception:
        # Browser exception messages may contain page HTML or signed URLs.
        print("markdown-table-image: rendering failed (input, filesystem, or browser error)", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
