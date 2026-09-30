//! Check `swarmy ...` and `swarmyd ...` commands named in markdown against the
//! binaries' real clap trees.
//!
//! The docs accuracy check runs in two halves. `scripts/check-docs-accuracy.py`
//! owns repository paths and `--test` targets, which are filesystem facts,
//! while this crate owns command names and flags. Each binary passes the tree
//! its own `clap::CommandFactory` reports, so renaming a subcommand or a flag
//! without updating the docs fails the test. Extraction and validation stay in
//! this one place; the two binaries only supply their trees.
//!
//! A documented invocation is every `swarmy` or `swarmyd` word in a backticked
//! span or a fenced code block, plus `cargo run -p <package> -- <args>` lines
//! for the mapped packages. Quoted strings inside fenced blocks are masked
//! first, so prose like `echo "run swarmy remote ..."` is not read as an
//! invocation. Files under `backlog/` are proposals and are never checked.

use std::path::{Path, PathBuf};

use clap::{Arg, ArgAction, Command};
use thiserror::Error;

/// A command the clap tree does not define, or a failure to read the checkout.
#[derive(Debug, Error)]
pub enum Error {
    /// `git ls-files` could not start.
    #[error("cannot run git ls-files: {0}")]
    Spawn(std::io::Error),
    /// `git ls-files` exited nonzero or its output was not UTF-8.
    #[error("cannot list tracked markdown files: {0}")]
    FileList(String),
    /// A tracked markdown file could not be read.
    #[error("cannot read {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    /// Every unknown documented command, one per line.
    #[error("{count} unknown documented command(s):\n{details}")]
    Unknown { count: usize, details: String },
}

/// Check every documented invocation of `binaries` in the checkout at
/// `repo_root`. `cargo_runs` maps a cargo package name to the binary whose
/// tree validates the arguments after its `--`, for example
/// `("swarmy-cli", "swarmy")`. Returns the number of invocations checked.
///
/// Binaries this call does not name are skipped, so each binary's test only
/// fails on its own commands.
///
/// # Errors
///
/// Returns an error when the tracked files cannot be listed or read, or when
/// a documented command names no subcommand or flag in the clap tree.
pub fn check_repo(
    repo_root: &Path,
    binaries: &[(&str, &Command)],
    cargo_runs: &[(&str, &str)],
) -> Result<usize, Error> {
    let files = tracked_markdown(repo_root)?;
    let mut contents = Vec::with_capacity(files.len());
    for relative in files {
        let content =
            std::fs::read_to_string(repo_root.join(&relative)).map_err(|source| Error::Read {
                path: relative.clone(),
                source,
            })?;
        contents.push((relative, content));
    }
    check_files(&contents, binaries, cargo_runs)
}

/// Check in-memory markdown files, each a relative path and its content.
/// This is the pure core of [`check_repo`]; unit tests drive it directly.
///
/// # Errors
///
/// Returns [`Error::Unknown`] listing every documented command the clap tree
/// does not define.
pub fn check_files(
    files: &[(PathBuf, String)],
    binaries: &[(&str, &Command)],
    cargo_runs: &[(&str, &str)],
) -> Result<usize, Error> {
    // Build the trees the way parsing does, so the automatic `--help` and
    // `--version` flags validate the same way they run.
    let built: Vec<(String, Command)> = binaries
        .iter()
        .map(|(name, command)| {
            let mut built = (*command).clone();
            built.build();
            ((*name).to_owned(), built)
        })
        .collect();
    let binaries: Vec<(&str, &Command)> = built
        .iter()
        .map(|(name, command)| (name.as_str(), command))
        .collect();
    let mut problems: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut checked = 0;
    for (relative, content) in files {
        for logical in logical_lines(content) {
            let location = format!("{}:{}", relative.display(), logical.lineno);
            for span in backtick_spans(&logical.text) {
                let text = span.trim();
                if !text.is_empty() {
                    checked += scan_text(
                        text,
                        &location,
                        &binaries,
                        cargo_runs,
                        &mut problems,
                        &mut seen,
                    );
                }
            }
            if logical.in_fence {
                checked += scan_text(
                    &mask_quoted(&logical.text),
                    &location,
                    &binaries,
                    cargo_runs,
                    &mut problems,
                    &mut seen,
                );
            }
        }
    }
    if problems.is_empty() {
        Ok(checked)
    } else {
        let count = problems.len();
        Err(Error::Unknown {
            count,
            details: problems.join("\n"),
        })
    }
}

/// Tracked markdown files, relative to `repo_root`, without proposals.
fn tracked_markdown(repo_root: &Path) -> Result<Vec<PathBuf>, Error> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(["ls-files", "-z"])
        .output()
        .map_err(Error::Spawn)?;
    if !output.status.success() {
        let detail = String::from_utf8_lossy(&output.stderr).into_owned();
        return Err(Error::FileList(detail));
    }
    let text =
        String::from_utf8(output.stdout).map_err(|error| Error::FileList(error.to_string()))?;
    let mut files: Vec<PathBuf> = text
        .split('\0')
        .filter(|path| {
            Path::new(path).extension().is_some_and(|ext| ext == "md")
                && !path.starts_with("backlog/")
        })
        .map(PathBuf::from)
        .collect();
    files.sort();
    Ok(files)
}

/// One line of markdown with its fence state, joining backslash continuations.
struct Logical {
    lineno: usize,
    text: String,
    in_fence: bool,
}

fn logical_lines(content: &str) -> Vec<Logical> {
    let mut lines = Vec::new();
    let mut in_fence = false;
    let mut buffer = String::new();
    let mut start = 0;
    for (index, line) in content.split('\n').enumerate() {
        let stripped = line.trim();
        if stripped.starts_with("```") && stripped.matches("```").count() == 1 {
            if !buffer.is_empty() {
                lines.push(Logical {
                    lineno: start,
                    text: std::mem::take(&mut buffer),
                    in_fence: true,
                });
            }
            lines.push(Logical {
                lineno: index + 1,
                text: line.to_owned(),
                in_fence: false,
            });
            in_fence = !in_fence;
            continue;
        }
        if in_fence && line.trim_end().ends_with('\\') {
            if buffer.is_empty() {
                start = index + 1;
            } else {
                buffer.push(' ');
            }
            buffer.push_str(line.trim_end().trim_end_matches('\\'));
            continue;
        }
        if !buffer.is_empty() {
            buffer.push(' ');
            buffer.push_str(line);
            lines.push(Logical {
                lineno: start,
                text: std::mem::take(&mut buffer),
                in_fence: true,
            });
            continue;
        }
        lines.push(Logical {
            lineno: index + 1,
            text: line.to_owned(),
            in_fence,
        });
    }
    if !buffer.is_empty() {
        lines.push(Logical {
            lineno: start,
            text: buffer,
            in_fence: true,
        });
    }
    lines
}

/// The odd segments of a backtick split: sequential non-overlapping pairs.
fn backtick_spans(line: &str) -> Vec<&str> {
    line.split('`')
        .enumerate()
        .filter(|(index, _)| index % 2 == 1)
        .map(|(_, span)| span)
        .collect()
}

/// Blank the inside of `"..."` spans so quoted prose is not read as commands.
fn mask_quoted(line: &str) -> String {
    let mut masked = String::with_capacity(line.len());
    let mut in_quotes = false;
    let mut escaped = false;
    for c in line.chars() {
        if in_quotes {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_quotes = false;
                masked.push(c);
                continue;
            }
            masked.push(' ');
        } else if c == '"' {
            in_quotes = true;
            masked.push(c);
        } else {
            masked.push(c);
        }
    }
    masked
}

/// Validate every invocation on one line; returns the number found.
#[allow(
    clippy::too_many_arguments,
    reason = "one call site threads the check state through"
)]
fn scan_text(
    text: &str,
    location: &str,
    binaries: &[(&str, &Command)],
    cargo_runs: &[(&str, &str)],
    problems: &mut Vec<String>,
    seen: &mut std::collections::HashSet<String>,
) -> usize {
    let mut checked = 0;
    if let Some((binary, rest)) = cargo_run(text, cargo_runs) {
        checked += 1;
        check_invocation(binary, rest, text, location, binaries, problems, seen);
    }
    for (binary, rest) in find_invocations(text) {
        checked += 1;
        check_invocation(binary, rest, text, location, binaries, problems, seen);
    }
    checked
}

/// A `cargo run -p <package> -- <args>` line checks as its mapped binary.
fn cargo_run<'line, 'pkg>(
    line: &'line str,
    packages: &[(&'pkg str, &'pkg str)],
) -> Option<(&'pkg str, &'line str)> {
    let words = word_offsets(line);
    let mut cursor = 0;
    cursor = find_word(&words, cursor, "cargo")?;
    cursor = find_word(&words, cursor, "run")?;
    cursor = find_word(&words, cursor, "-p")?;
    let package = words.get(cursor + 1)?.2;
    let binary = packages.iter().find(|(name, _)| *name == package)?.1;
    cursor = find_word(&words, cursor + 1, "--")?;
    Some((binary, line[words[cursor].1..].trim()))
}

/// Whitespace-separated words with their byte offsets.
fn word_offsets(line: &str) -> Vec<(usize, usize, &str)> {
    let mut words = Vec::new();
    let mut start: Option<usize> = None;
    for (index, c) in line.char_indices() {
        if c.is_whitespace() {
            if let Some(first) = start.take() {
                words.push((first, index, &line[first..index]));
            }
        } else if start.is_none() {
            start = Some(index);
        }
    }
    if let Some(first) = start {
        words.push((first, line.len(), &line[first..]));
    }
    words
}

/// The first index at or after `cursor` whose word matches, or `None`.
fn find_word(words: &[(usize, usize, &str)], cursor: usize, want: &str) -> Option<usize> {
    words
        .iter()
        .enumerate()
        .skip(cursor)
        .find(|(_, word)| word.2 == want)
        .map(|(index, _)| index)
}

/// Every `swarmy` or `swarmyd` invocation start on the line: a command boundary
/// with an optional `path/to/` prefix, naming the binary, not continued by a
/// word character (so `swarmy-version` never matches).
fn find_invocations(line: &str) -> Vec<(&str, &str)> {
    let mut found = Vec::new();
    let mut index = 0;
    while index < line.len() {
        if index == 0 || is_invocation_boundary(prev_char(line, index)) {
            // Candidate starts are the boundary itself plus every position
            // after a `/` in the path-char run, longest prefix first.
            let mut cursor = index;
            let mut starts = vec![index];
            while next_char(line, cursor).is_some_and(is_path_char) {
                let next = cursor + next_char(line, cursor).map_or(0, char_len);
                if next_char(line, cursor) == Some('/') {
                    starts.push(next);
                }
                cursor = next;
            }
            let mut matched = false;
            for from in starts.into_iter().rev() {
                let binary = ["swarmyd", "swarmy"]
                    .into_iter()
                    .find(|name| line[from..].starts_with(name));
                if let Some(binary) = binary {
                    let end = from + binary.len();
                    let continued = next_char(line, end)
                        .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
                    if !continued {
                        found.push((binary, &line[end..]));
                        index = end;
                        matched = true;
                        break;
                    }
                }
            }
            if matched {
                continue;
            }
        }
        index += next_char(line, index).map_or(1, char_len);
    }
    found
}

/// The character just before byte `index`, which stays on a boundary.
fn prev_char(line: &str, index: usize) -> char {
    line[..index].chars().next_back().unwrap_or('\0')
}

/// The character at byte `index`, if any.
fn next_char(line: &str, index: usize) -> Option<char> {
    line[index..].chars().next()
}

/// The UTF-8 width of one character.
fn char_len(c: char) -> usize {
    c.len_utf8()
}

/// Characters that can precede a command word on a shell line.
fn is_invocation_boundary(c: char) -> bool {
    c.is_whitespace() || matches!(c, '\'' | '"' | ';' | '(' | '|' | '&')
}

/// Characters of an optional `path/to/binary` prefix before the binary name.
fn is_path_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '/' | '$' | '~' | '-')
}

/// One documented invocation and where it was found.
struct Context<'state> {
    source: &'state str,
    location: String,
    problems: &'state mut Vec<String>,
    seen: &'state mut std::collections::HashSet<String>,
}

/// Validate one invocation against its binary's tree, if that binary is ours.
#[allow(
    clippy::too_many_arguments,
    reason = "one call site threads the check state through"
)]
fn check_invocation(
    binary: &str,
    rest: &str,
    source: &str,
    location: &str,
    binaries: &[(&str, &Command)],
    problems: &mut Vec<String>,
    seen: &mut std::collections::HashSet<String>,
) {
    let Some((_, root)) = binaries.iter().find(|(name, _)| *name == binary) else {
        return;
    };
    let before = rest.split_once(" #").map_or(rest, |(head, _)| head);
    let tokens = split_args(before);
    if tokens.is_empty() || tokens[0].starts_with('/') {
        return;
    }
    if !tokens[0].starts_with('-') && !is_command_word(&tokens[0]) {
        return;
    }
    let mut context = Context {
        source,
        location: location.to_owned(),
        problems,
        seen,
    };
    let mut stack = vec![*root];
    let mut trail = vec![binary.to_owned()];
    walk(&tokens, &mut stack, &mut trail, &mut false, &mut context);
}

/// Record one unknown command or flag, once per distinct message.
fn report(context: &mut Context<'_>, message: &str) {
    let line = format!("{}: {message}: {}", context.location, context.source.trim());
    if context.seen.insert(line.clone()) {
        context.problems.push(line);
    }
}

/// Shell-ish words; quoted spans stay one token so values survive splitting.
fn split_args(text: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if let Some(open) = quote {
            if c == '\\' {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            } else if c == open {
                quote = None;
            } else {
                current.push(c);
            }
        } else {
            match c {
                '\'' | '"' => quote = Some(c),
                '\\' => {
                    if let Some(next) = chars.next() {
                        current.push(next);
                    }
                }
                c if c.is_whitespace() => {
                    if !current.is_empty() {
                        words.push(std::mem::take(&mut current));
                    }
                }
                _ => current.push(c),
            }
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

/// A token that can name a subcommand: a word, not punctuation or a value.
fn is_command_word(token: &str) -> bool {
    let mut chars = token.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() => (),
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '/' | '-'))
}

/// Walk the tokens down the clap tree, checking subcommand names and flags.
/// Past a leaf, bare words are arguments and only `--flag` spellings are
/// still checked, so a renamed flag fails even after a positional.
fn walk(
    tokens: &[String],
    stack: &mut Vec<&Command>,
    trail: &mut Vec<String>,
    skip_next: &mut bool,
    context: &mut Context<'_>,
) {
    for token in tokens {
        if *skip_next {
            *skip_next = false;
            continue;
        }
        if token == "--" {
            return;
        }
        if let Some(name) = token.strip_prefix("--") {
            if check_flag(name, skip_next, context, stack) {
                return;
            }
            continue;
        }
        if token.starts_with('-') {
            // Short flags end static checking: values like `-f` (as in
            // `journalctl -u swarmyd -f`) are indistinguishable from typos.
            return;
        }
        if token == "..." || token == "…" {
            return;
        }
        let Some(current) = stack.last().copied() else {
            return;
        };
        if let Some(child) = find_sub(current, token) {
            trail.push(token.clone());
            stack.push(child);
        } else if current.get_subcommands().next().is_some() {
            trail.push(token.clone());
            report(context, &format!("unknown command '{}'", trail.join(" ")));
            return;
        }
    }
}

/// Check one `--flag` token against the current node and its ancestors, so
/// global flags validate at any depth. Returns true when the walk must stop:
/// an unknown flag was reported, or `--test` handed the line to the path
/// check, which owns integration-test targets as filesystem facts.
fn check_flag(
    name: &str,
    skip_next: &mut bool,
    context: &mut Context<'_>,
    stack: &[&Command],
) -> bool {
    let flag = name.split('=').next().unwrap_or("");
    if flag == "test" {
        return true;
    }
    match find_flag(stack, flag) {
        None => {
            report(context, &format!("unknown flag '--{flag}'"));
            true
        }
        Some(arg) => {
            if takes_value(arg) && !name.contains('=') {
                *skip_next = true;
            }
            false
        }
    }
}

/// A flag by its long name on the current node or any ancestor (globals).
fn find_flag<'tree>(stack: &[&'tree Command], name: &str) -> Option<&'tree Arg> {
    stack.iter().rev().find_map(|node| {
        node.get_arguments()
            .find(|arg| arg.get_long() == Some(name))
    })
}

/// A flag that consumes the following token as its value.
fn takes_value(arg: &Arg) -> bool {
    matches!(arg.get_action(), ArgAction::Set | ArgAction::Append)
        || arg.get_num_args().is_some_and(|count| count.takes_values())
}

/// A child subcommand by name or alias.
fn find_sub<'tree>(node: &'tree Command, token: &str) -> Option<&'tree Command> {
    node.get_subcommands().find(|child| {
        child.get_name() == token
            || child.get_aliases().any(|alias| alias == token)
            || child.get_visible_aliases().any(|alias| alias == token)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Command {
        Command::new("swarmy")
            .arg(
                Arg::new("json")
                    .long("json")
                    .action(ArgAction::SetTrue)
                    .global(true),
            )
            .arg(
                Arg::new("remote")
                    .long("remote")
                    .action(ArgAction::Set)
                    .global(true),
            )
            .subcommand(
                Command::new("widget")
                    .subcommand(Command::new("ls").alias("list"))
                    .subcommand(
                        Command::new("show")
                            .arg(Arg::new("format").long("format").action(ArgAction::Set)),
                    ),
            )
            .subcommand(
                Command::new("run").arg(Arg::new("queue").long("queue").action(ArgAction::Set)),
            )
            .subcommand(
                Command::new("nested")
                    .subcommand(Command::new("deep").subcommand(Command::new("run"))),
            )
    }

    fn file(content: &str) -> Vec<(PathBuf, String)> {
        vec![(PathBuf::from("docs/page.md"), content.to_owned())]
    }

    fn check(content: &str) -> Result<usize, Error> {
        let tree = fixture();
        check_files(&file(content), &[("swarmy", &tree)], &[])
    }

    fn message(error: &Error) -> String {
        error.to_string()
    }

    #[test]
    fn valid_commands_pass() {
        let content = [
            "`swarmy widget ls`, `swarmy widget list`, `swarmy widget show w1`,",
            "`swarmy widget show w1 --format short`, `swarmy --json widget ls`,",
            "`swarmy widget show w1 --remote dev`, `swarmy --remote=dev run`,",
            "`swarmy widget`, `swarmy`, and `./target/debug/swarmy widget ls`.",
            "",
            "```sh",
            "swarmy run --queue text",
            "swarmy nested deep run PROMPT",
            "```",
        ]
        .join("\n");
        check(&content).expect("documented commands exist");
    }

    #[test]
    fn unknown_subcommands_fail() {
        for (case, want) in [
            (
                "`swarmy widget bogus`",
                "unknown command 'swarmy widget bogus'",
            ),
            ("`swarmy nosuch`", "unknown command 'swarmy nosuch'"),
            (
                "`swarmy nested deep bogus`",
                "unknown command 'swarmy nested deep bogus'",
            ),
        ] {
            let error = check(case).expect_err("unknown subcommand must fail");
            assert!(
                message(&error).contains(want),
                "missing {want:?} for {case:?}"
            );
        }
    }

    #[test]
    fn unknown_flags_fail_even_after_positionals() {
        for (case, want) in [
            ("`swarmy run --jsn`", "unknown flag '--jsn'"),
            (
                "`swarmy widget show w1 --formt short`",
                "unknown flag '--formt'",
            ),
        ] {
            let error = check(case).expect_err("unknown flag must fail");
            assert!(
                message(&error).contains(want),
                "missing {want:?} for {case:?}"
            );
        }
    }

    #[test]
    fn test_targets_and_prose_are_not_commands() {
        // `--test TARGET` belongs to the path check; journalctl flags, paths,
        // and table pipes only name an invocation with a real first word.
        let content = [
            "Run `swarmyd --test vol` on the node, `swarmy --test whatever`,",
            "and `journalctl -u swarmy -f`, plus `swarmy /config.toml`,",
            "`swarmy | swarmy |`, and `swarmy ...`.",
            "",
            "```sh",
            "echo \"run swarmy widget bogus ...\"",
            "```",
        ]
        .join("\n");
        check(&content).expect("prose must not fail the command check");
    }

    #[test]
    fn cargo_run_uses_the_mapped_binary() {
        let tree = fixture();
        let files = file("```sh\ncargo run -p swarmy-cli -- widget ls\n```");
        check_files(&files, &[("swarmy", &tree)], &[("swarmy-cli", "swarmy")])
            .expect("mapped cargo run must pass");
        let files = file("```sh\ncargo run -p swarmy-cli -- widget bogus\n```");
        let error = check_files(&files, &[("swarmy", &tree)], &[("swarmy-cli", "swarmy")])
            .expect_err("mapped cargo run must fail on unknown commands");
        assert!(message(&error).contains("unknown command 'swarmy widget bogus'"));
        let files = file("```sh\ncargo run -p other -- widget bogus\n```");
        check_files(&files, &[("swarmy", &tree)], &[("swarmy-cli", "swarmy")])
            .expect("unmapped packages are out of scope");
    }

    #[test]
    fn comments_and_continuations_join() {
        check("`swarmy widget ls # lists widgets`").expect("comments are not args");
        let content = "```sh\nswarmy widget \\\n    bogus\n```";
        let error = check(content).expect_err("joined lines must fail together");
        assert!(message(&error).contains("unknown command 'swarmy widget bogus'"));
    }
}
