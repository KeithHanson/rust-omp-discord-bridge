//! Coba's fixed relevance question and lazy, exactly token-budgeted note passages.
//!
//! Tokenizer and decision metadata are unmodified files from
//! https://huggingface.co/ollaya-dev/coba-150m-ternary/tree/b5f7c77b93bc83559b1549c32026ee834a23280f
//! (`tokenizer/tokenizer.json`, also byte-identical to the root tokenizer.json).
//! Tokenizer SHA-256: 6c8aaa9a542084f2457eab775d4eeb51f92a70c0fd9de28d5edb0ddec3c08d30.
//! Upstream Apache-2.0 LICENSE and NOTICE (including MIT attribution) accompany the assets.
//! Layout and option rendering follow ollaya v0.18.0, crates/ollaya-decision/src/{layout,question}.rs:
//! [CLS] head [SEP] [MASK] false-option [MASK] true-option [SEP] state [SEP].

use std::{borrow::Cow, sync::OnceLock};

use anyhow::{Result, anyhow, bail, ensure};
use serde_json::{Value, json};
use tokenizers::{Encoding, Tokenizer};

pub const MODEL: &str = "coba:150m-ternary";
pub const THRESHOLD: f64 = 0.50;
const QUESTION: &str = "Is this passage useful for this TODO?";
const FALSE_CRITERION: &str = "Not useful";
const TRUE_CRITERION: &str = "Useful";
const MASK: &str = "[MASK]";
const MAX_TOKENS: usize = 512;
const HEAD_MAX_TOKENS: usize = 192;
const OVERLAP_TOKENS: usize = 32;
// Keep useful body room instead of silently truncating an oversized TODO or title.
const MIN_PASSAGE_TOKENS: usize = 32;
// A work/memory bound, NOT a token estimate. Every resulting state is encoded exactly.
const LOOKAHEAD_BYTES: usize = 8192;

pub fn request(state: String) -> Value {
    json!({
        "model": MODEL,
        "state": state,
        "questions": {
            "relevant": {
                "type": "noul",
                "instructions": QUESTION,
                "criteria": {"false": FALSE_CRITERION, "true": TRUE_CRITERION}
            }
        }
    })
}

struct Runtime {
    tokenizer: Tokenizer,
    state_budget: usize,
}

impl Runtime {
    fn load() -> Result<Self> {
        let tokenizer = Tokenizer::from_bytes(include_bytes!("../assets/coba/tokenizer.json"))
            .map_err(|error| anyhow!("could not load bundled Coba tokenizer: {error}"))?;
        let head = encode(&tokenizer, &format!("noul question: {QUESTION}"))?.len();
        let false_option = encode(&tokenizer, &format!(" false: {FALSE_CRITERION}"))?.len();
        let true_option = encode(&tokenizer, &format!(" true: {TRUE_CRITERION}"))?.len();
        ensure!(
            false_option <= 48 && true_option <= 48
                && head + false_option + true_option + 2 <= HEAD_MAX_TOKENS,
            "fixed Coba question exceeds the runtime head budget"
        );
        // One CLS, three SEPs and the two option MASKs are separate token IDs.
        let state_budget = MAX_TOKENS - (head + false_option + true_option + 6);
        Ok(Self { tokenizer, state_budget })
    }
}

fn runtime() -> Result<&'static Runtime> {
    static RUNTIME: OnceLock<Result<Runtime, String>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| Runtime::load().map_err(|error| error.to_string()))
        .as_ref()
        .map_err(|error| anyhow!("{error}"))
}

fn encode(tokenizer: &Tokenizer, text: &str) -> Result<Encoding> {
    tokenizer.encode(text, false)
        .map_err(|error| anyhow!("could not tokenize Coba state: {error}"))
}

fn clean_mask(text: &str) -> Cow<'_, str> {
    if text.contains(MASK) {
        Cow::Owned(text.replace(MASK, " "))
    } else {
        Cow::Borrowed(text)
    }
}

// The tokenizer returns byte offsets in its input, before its NFC normalization.
// Undo the runtime's six-byte [MASK] -> one-byte space replacement for slicing.
fn original_offset(text: &str, mut offset: usize) -> usize {
    for (start, _) in text.match_indices(MASK) {
        if start >= offset {
            break;
        }
        offset += MASK.len() - 1;
    }
    offset
}

// Byte-level tokens can share a Unicode scalar's offsets. Never cut a scalar or
// a literal mask, including when the bounded lookahead ends inside either one.
fn boundary(text: &str, mut offset: usize) -> usize {
    while !text.is_char_boundary(offset) {
        offset -= 1;
    }
    for back in 1..MASK.len().min(offset + 1) {
        let start = offset - back;
        if text.is_char_boundary(start) && text[start..].starts_with(MASK) {
            return start;
        }
    }
    offset
}

/// Borrows the note body; tokenizes at most one bounded lookahead at a time.
/// Dropping this iterator after a useful passage leaves the rest unprocessed.
/// Emitted strings retain the original body bytes, including masks and newlines.
/// The server's mask replacement is applied only for counting and offset mapping.
pub struct NotePassages<'a> {
    runtime: &'static Runtime,
    prefix: String,
    clean_prefix_len: usize,
    content: &'a str,
    start: usize,
    done: bool,
}

impl<'a> NotePassages<'a> {
    pub fn new(todo: &str, title: &str, content: &'a str) -> Result<Self> {
        let runtime = runtime()?;
        let prefix = format!("TODO:\n{todo}\n\nNOTE_TITLE:\n{title}\n\nNOTE_PASSAGE:\n");
        let clean_prefix = clean_mask(&prefix);
        let prefix_tokens = encode(&runtime.tokenizer, &clean_prefix)?.len();
        ensure!(
            prefix_tokens + MIN_PASSAGE_TOKENS <= runtime.state_budget,
            "Coba TODO/title prefix is too large: {prefix_tokens} tokens leaves fewer than \
             {MIN_PASSAGE_TOKENS} passage tokens in the {}-token state budget",
            runtime.state_budget
        );
        let clean_prefix_len = clean_prefix.len();
        Ok(Self { runtime, prefix, clean_prefix_len, content, start: 0, done: false })
    }

    fn next_state(&mut self) -> Result<String> {
        let remaining = &self.content[self.start..];
        let end = boundary(remaining, remaining.len().min(LOOKAHEAD_BYTES));
        let mut state = String::with_capacity(self.prefix.len() + end);
        state.push_str(&self.prefix);
        state.push_str(&remaining[..end]);
        loop {
            let clean = clean_mask(&state);
            let encoding = encode(&self.runtime.tokenizer, &clean)?;
            if encoding.len() <= self.runtime.state_budget {
                let body_len = state.len() - self.prefix.len();
                let end = self.start + body_len;
                if end == self.content.len() {
                    self.done = true;
                } else {
                    ensure!(body_len > 0, "Coba TODO/title leaves no room for a note passage");
                    let body_tokens = encoding.get_offsets().iter()
                        .filter(|&&(_, end)| end > self.clean_prefix_len)
                        .count();
                    // For tiny windows reduce overlap so every iteration advances.
                    let overlap = OVERLAP_TOKENS.min(body_tokens / 2);
                    let advance = if overlap == 0 {
                        body_len
                    } else {
                        let offset = encoding.get_offsets()[encoding.len() - overlap].0;
                        let original = original_offset(&state, offset);
                        boundary(&state, original).saturating_sub(self.prefix.len())
                    };
                    self.start += if advance == 0 { body_len } else { advance };
                }
                return Ok(state);
            }
            let offset = encoding.get_offsets()[self.runtime.state_budget].0;
            let cut = boundary(&state, original_offset(&state, offset));
            // Re-encode the shortened state: a BPE/NFC boundary may change the
            // final token count, so offsets alone are never the budget check.
            let cut = if cut > self.prefix.len() && cut < state.len() {
                cut
            } else {
                // NFC can map many tokens to the same original character span.
                // If that gives no usable cut, reduce the body and encode again.
                let body = &state[self.prefix.len()..];
                let first = if body.starts_with(MASK) {
                    MASK.len()
                } else {
                    body.chars().next().map_or(0, char::len_utf8)
                };
                let shorter = boundary(body, body.len() / 2).max(first);
                if shorter == 0 || shorter >= body.len() {
                    bail!("Coba TODO/title leaves no room for a complete note character");
                }
                self.prefix.len() + shorter
            };
            state.truncate(cut);
        }
    }
}

impl Iterator for NotePassages<'_> {
    type Item = Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let state = self.next_state();
        if state.is_err() {
            self.done = true;
        }
        Some(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Assemble IDs independently, exactly as the verified ollaya layout does.
    fn packed_ids(state: &str) -> Vec<u32> {
        let tokenizer = &runtime().unwrap().tokenizer;
        let mut ids = vec![50281];
        ids.extend(encode(tokenizer, &format!("noul question: {QUESTION}")).unwrap().get_ids());
        ids.extend([50282, 50284]);
        ids.extend(encode(tokenizer, " false: Not useful").unwrap().get_ids());
        ids.push(50284);
        ids.extend(encode(tokenizer, " true: Useful").unwrap().get_ids());
        ids.push(50282);
        ids.extend(encode(tokenizer, &state.replace(MASK, " ")).unwrap().get_ids());
        ids.push(50282);
        ids
    }

    fn check_coverage(todo: &str, title: &str, content: &str) -> (usize, usize) {
        let mut passages = NotePassages::new(todo, title, content).unwrap();
        let mut covered = 0;
        let mut count = 0;
        let mut largest = 0;
        while !passages.done {
            let start = passages.start;
            let state = passages.next().unwrap().unwrap();
            let body = &state[passages.prefix.len()..];
            let end = start + body.len();
            assert_eq!(body, &content[start..end]);
            assert!(start <= covered, "gap in note coverage");
            if !passages.done {
                assert!(passages.start > start, "iterator did not advance");
                assert!(passages.start <= end, "overlap skipped original text");
            }
            covered = covered.max(end);
            let packed_len = packed_ids(&state).len();
            assert!(packed_len <= MAX_TOKENS, "runtime would truncate state");
            largest = largest.max(packed_len);
            count += 1;
            assert!(count <= content.chars().count().max(1));
        }
        assert_eq!(covered, content.len());
        assert!(passages.next().is_none());
        (count, largest)
    }

    #[test]
    fn exact_packed_budget_and_token_overlap() {
        let content = "word ".repeat(2000);
        let (count, largest) = check_coverage("Find useful words", "Words", &content);
        assert!(count > 1);
        assert_eq!(largest, MAX_TOKENS);
        let mut passages = NotePassages::new("Find useful words", "Words", &content).unwrap();
        let state = passages.next().unwrap().unwrap();
        let end = state.len() - passages.prefix.len();
        assert!(passages.start < end);
        let overlap = &content[passages.start..end];
        let overlap_tokens = encode(&runtime().unwrap().tokenizer, overlap).unwrap().len();
        assert!((OVERLAP_TOKENS - 1..=OVERLAP_TOKENS + 1).contains(&overlap_tokens));
    }

    #[test]
    fn unicode_newlines_and_repeated_masks_remain_complete() {
        for content in [
            "日本語 🦀 e\u{301}\n\n[MASK][MASK] [MASK]résumé\r\n".repeat(1000),
            "[MASK]".repeat(3000),
            "🦀".repeat(3000),
            " \n\t".repeat(4000),
            format!("e{}", "\u{301}\u{302}".repeat(3000)),
        ] {
            let (count, _) = check_coverage("調べる [MASK]", "é[MASK]e\u{301}", &content);
            assert!(count > 1);
        }
    }

    #[test]
    fn empty_content_yields_one_title_based_request() {
        let mut passages = NotePassages::new("Find notes", "Relevant title", "").unwrap();
        let state = passages.next().unwrap().unwrap();
        assert_eq!(state, "TODO:\nFind notes\n\nNOTE_TITLE:\nRelevant title\n\nNOTE_PASSAGE:\n");
        assert!(packed_ids(&state).len() <= MAX_TOKENS);
        let payload = request(state.clone());
        assert_eq!(payload["state"], state);
        assert_eq!(payload["model"], MODEL);
        assert_eq!(payload["questions"]["relevant"]["instructions"], QUESTION);
        assert!(passages.next().is_none());
    }

    #[test]
    fn oversized_fixed_fields_are_errors_not_truncation() {
        let oversized = "word ".repeat(1000);
        for (todo, title) in [(oversized.as_str(), "title"), ("todo", oversized.as_str())] {
            let error = NotePassages::new(todo, title, "small body").err().unwrap();
            assert!(error.to_string().contains("TODO/title prefix is too large"));
        }
    }

    #[test]
    fn bundled_metadata_matches_runtime_layout() {
        let metadata: Value = serde_json::from_slice(include_bytes!("../assets/coba/decision.json")).unwrap();
        assert_eq!(metadata["max_len"], MAX_TOKENS);
        assert_eq!(metadata["head_max_len"], HEAD_MAX_TOKENS);
        let tokenizer = &runtime().unwrap().tokenizer;
        for (name, text) in [("cls", "[CLS]"), ("sep", "[SEP]"), ("mask", MASK), ("pad", "[PAD]")] {
            assert_eq!(metadata["special_tokens"][name], tokenizer.token_to_id(text).unwrap());
        }
        assert!(tokenizer.get_truncation().is_none());
        assert!(tokenizer.get_padding().is_none());
    }
}
