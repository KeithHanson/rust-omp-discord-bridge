pub fn thread_name(prompt: &str) -> String {
    let mut output = String::new();
    let mut previous_space = false;
    for character in prompt.chars() {
        let normalized = if character.is_control() {
            ' '
        } else {
            character
        };
        if normalized.is_whitespace() {
            if !previous_space && !output.is_empty() {
                output.push(' ');
            }
            previous_space = true;
        } else {
            output.push(normalized);
            previous_space = false;
        }
        if output.encode_utf16().count() >= 90 {
            break;
        }
    }
    let trimmed = output.trim().trim_matches(['-', '_']).trim();
    let name = if trimmed.is_empty() {
        "omp-session".to_owned()
    } else {
        trimmed.to_owned()
    };
    if name.encode_utf16().count() < 2 {
        format!("{name}-session")
    } else {
        name
    }
}

/// Renders the human-facing bodies of a trigger's full prompt for the session
/// thread. The channel headline keeps only a short preview; humans reading the
/// thread get the complete prompt, chunked at Discord's message limit.
pub fn trigger_prompt_messages(prompt: &str, limit: usize) -> Vec<String> {
    chunk_message(&format!("**Prompt:**\n\n{prompt}"), limit)
}

pub fn chunk_message(text: &str, limit: usize) -> Vec<String> {
    assert!(limit >= 100, "Discord message limit must be at least 100");
    if text.is_empty() {
        return vec!["(OMP completed without text output)".to_owned()];
    }

    if discord_len(text) <= limit {
        return vec![text.to_owned()];
    }

    let has_fence = text
        .lines()
        .any(|line| line.trim_start().starts_with("```"));
    let raw_limit = if has_fence { limit - 40 } else { limit };
    let mut raw_chunks = Vec::new();
    let mut remaining = text;
    while discord_len(remaining) > raw_limit {
        let split = split_index(remaining, raw_limit);
        let (chunk, rest) = remaining.split_at(split);
        raw_chunks.push(chunk.to_owned());
        remaining = rest;
    }
    if !remaining.is_empty() {
        raw_chunks.push(remaining.to_owned());
    }
    if !has_fence {
        return raw_chunks;
    }

    let mut open_fence = None;
    raw_chunks
        .into_iter()
        .map(|raw| {
            let prefix = open_fence
                .as_deref()
                .map_or_else(String::new, |language| format!("```{language}\n"));
            update_fence_state(&raw, &mut open_fence);
            let suffix = if open_fence.is_some() { "\n```" } else { "" };
            format!("{prefix}{raw}{suffix}")
        })
        .collect()
}

fn split_index(text: &str, limit: usize) -> usize {
    let mut units = 0;
    let mut hard_split = 0;
    let mut preferred_split = None;
    for (index, character) in text.char_indices() {
        let next_units = units + character.len_utf16();
        if next_units > limit {
            break;
        }
        units = next_units;
        hard_split = index + character.len_utf8();
        if character == '\n' || character.is_whitespace() {
            preferred_split = Some(hard_split);
        }
    }
    preferred_split
        .filter(|index| *index >= hard_split / 2)
        .unwrap_or(hard_split)
}

fn discord_len(text: &str) -> usize {
    text.encode_utf16().count()
}

fn update_fence_state(text: &str, open_fence: &mut Option<String>) {
    for line in text.lines() {
        let trimmed = line.trim_start();
        let Some(info) = trimmed.strip_prefix("```") else {
            continue;
        };
        if open_fence.is_some() {
            *open_fence = None;
        } else {
            *open_fence = Some(info.trim().chars().take(24).collect());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunking_preserves_unicode_content_and_limits() {
        let input = format!("{}\n{}", "🦀".repeat(80), "x".repeat(220));
        let chunks = chunk_message(&input, 100);

        assert!(chunks.iter().all(|chunk| discord_len(chunk) <= 100));
        assert_eq!(chunks.concat(), input);
    }

    #[test]
    fn chunking_closes_and_reopens_code_fences() {
        let input = format!("```rs\n{}\n```", "let value = 1;\n".repeat(30));
        let chunks = chunk_message(&input, 100);

        assert!(chunks.len() > 1);
        assert!(chunks.iter().all(|chunk| discord_len(chunk) <= 100));
        assert!(chunks.iter().all(|chunk| chunk.starts_with("```rs\n")));
        assert!(chunks.iter().all(|chunk| chunk.ends_with("```")));
    }

    #[test]
    fn trigger_prompt_messages_preserve_the_full_prompt() {
        let prompt = format!("first line\n{}", "x".repeat(2_500));
        let chunks = trigger_prompt_messages(&prompt, 100);

        assert!(chunks.len() > 1);
        assert!(chunks.iter().all(|chunk| discord_len(chunk) <= 100));
        assert_eq!(chunks.concat(), format!("**Prompt:**\n\n{prompt}"));
    }

    #[test]
    fn thread_names_are_nonempty_and_bounded() {
        assert_eq!(thread_name("\n\t"), "omp-session");
        assert!(discord_len(&thread_name(&"🦀".repeat(100))) <= 90);
    }
}
