"""Deterministic renderer tests; run with the renderer's uv dependencies."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("table_renderer", Path(__file__).with_name("markdown-table-image.py"))
renderer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(renderer)

TABLE = "| Name | Value |\n| --- | --- |\n| one | two |\n"


class TableRendererTests(unittest.TestCase):
    def test_fenced_table_is_prose(self):
        source = "```markdown\n" + TABLE + "```\n"
        self.assertEqual(renderer.extract_tables(source), [])
        self.assertEqual(renderer.manifest_parts(source, [], []), [{"text": source}])

    def test_order_and_exact_prose(self):
        source = "Before\r\n\r\n" + TABLE.replace("\n", "\r\n") + "\r\nMiddle\n\n" + TABLE + "\nAfter"
        tables = renderer.extract_tables(source)
        self.assertEqual(renderer.manifest_parts(source, tables, [["/tmp/one.png"], ["/tmp/two.png"]]), [
            {"text": "Before\r\n\r\n"}, {"image": "/tmp/one.png"},
            {"text": "\r\nMiddle\n\n"}, {"image": "/tmp/two.png"}, {"text": "\nAfter"},
        ])

    def test_links_and_images_preserved_without_fetchable_html(self):
        source = "| Name | Value |\n| --- | --- |\n| [link](https://example.org/a) | ![alt](https://example.org/image) |\n"
        tables = renderer.extract_tables(source)
        document = next(renderer.documents(tables[0], 1, "<script>title</script>", 12, 4))
        self.assertNotIn("<img", document)
        self.assertNotIn("<script>", document)
        self.assertIn("&lt;script&gt;title", document)
        parts = renderer.manifest_parts(source, tables, [["/tmp/a.png"]])
        self.assertEqual(parts[-1], {"text": "\nTable links:\n<https://example.org/a>\n<https://example.org/image>\n"})

    def test_raw_html_escaped(self):
        table = renderer.extract_tables(TABLE.replace("one", '<img src="https://example.org/x">'))[0]
        document = next(renderer.documents(table, 1, "Table", 12, 4))
        self.assertNotIn("<img", document)
        self.assertIn("&lt;img", document)

    def test_row_column_splits_repeat_header_and_first_column(self):
        source = "| Key | B | C | D | E |\n| --- | --- | --- | --- | --- |\n" + "".join(
            f"| key{i} | b{i} | c{i} | d{i} | e{i} |\n" for i in range(3)
        )
        table = renderer.extract_tables(source)[0]
        documents = list(renderer.documents(table, 1, "Title", 2, 3))
        self.assertEqual(renderer.page_count(table, 2, 3), 4)
        self.assertEqual(len(documents), 4)
        self.assertTrue(all(">Key</th>" in doc for doc in documents))
        self.assertIn("key0", documents[0])
        self.assertIn("key0", documents[2])
        self.assertIn("key2", documents[1])
        self.assertNotIn("key2", documents[0])

    def test_limits_rejected_before_browser(self):
        with self.assertRaises(renderer.RenderError):
            renderer.extract_tables("x" * (renderer.MAX_SOURCE_BYTES + 1))
        with self.assertRaisesRegex(renderer.RenderError, "64 image"):
            renderer.render((TABLE + "\n") * 65, Path("unused.png"), "Table", 12, 4, True)
        with self.assertRaises(renderer.RenderError):
            renderer.render(TABLE, Path("unused.png"), "Table", 0, 4, True)
        table = renderer.extract_tables(TABLE)[0]
        self.assertEqual(sum(renderer.page_count(table, 12, 4) for _ in range(64)), 64)


if __name__ == "__main__":
    unittest.main()
