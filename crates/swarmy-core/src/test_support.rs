//! Shared environment checks for integration tests that need the dev stack.
//!
//! Each helper does the whole skip decision so call sites stay one line:
//! the value comes back as `Some` when present, the test logs a skip and
//! gets `None` when a developer runs it without the stack, and a missing
//! required setting panics when `CI` is set so a broken stack setup fails
//! the suite instead of silently passing it.

/// Read a required dev-stack setting.
///
/// A value that is present but not valid UTF-8 panics everywhere: that is a
/// broken environment, not a missing stack.
///
/// # Panics
/// Panics when the setting is missing under `CI`, or when it is present but
/// not valid UTF-8.
#[must_use = "check the returned option: missing stack settings skip the test locally"]
pub fn stack_env(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => missing_stack(name),
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("{name} is set but is not valid UTF-8")
        }
    }
}

/// Check a required dev-stack setting without converting it to UTF-8.
///
/// # Panics
/// Panics when the setting is missing under `CI`.
#[must_use = "check the returned option: missing stack settings skip the test locally"]
pub fn stack_env_os(name: &str) -> Option<std::ffi::OsString> {
    match std::env::var_os(name) {
        Some(value) => Some(value),
        None => missing_stack(name),
    }
}

/// Read an optional fixture setting such as a root-built test image.
/// Missing values skip even under `CI` because kernel-only suites are not
/// provisioned on hosted runners.
#[must_use = "check the returned option: missing optional settings skip the test"]
pub fn optional_env(name: &str) -> Option<String> {
    if let Ok(value) = std::env::var(name) {
        Some(value)
    } else {
        eprintln!("skipping integration test: optional {name} is unavailable");
        None
    }
}

/// Check an opt-in flag that must equal `1`, such as `SWARMY_API_FAKE_BENCH`.
/// Disabled flags skip even under `CI`; the hint must say how to opt in.
#[must_use = "check the returned option: disabled opt-in flags skip the test"]
pub fn opt_in_env(name: &str, hint: &str) -> Option<String> {
    if let Ok("1") = std::env::var(name).as_deref() {
        Some("1".to_owned())
    } else {
        eprintln!("skipping opt-in integration test: {hint}");
        None
    }
}

fn missing_stack<T>(name: &str) -> Option<T> {
    assert!(
        std::env::var_os("CI").is_none(),
        "CI requires {name} for integration tests"
    );
    eprintln!("skipping integration test: {name} is unavailable");
    None
}

/// Check documented invocations against the real clap trees. The Python
/// extractor prints location, binary, `\x1f`-joined args, and source per line.
///
/// # Errors
/// Returns the joined unknown commands and flags when docs name none in the tree.
#[cfg(feature = "test-support")]
pub fn check_docs_commands(
    repo_root: &std::path::Path,
    binaries: &[(&str, &clap::Command)],
) -> Result<usize, String> {
    let out = std::process::Command::new("python3")
        .args(["scripts/check-docs-accuracy.py", "--commands"])
        .current_dir(repo_root)
        .output()
        .map_err(|e| format!("cannot run docs extractor: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "docs extractor failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    let text = String::from_utf8(out.stdout).map_err(|e| format!("docs extractor output: {e}"))?;
    let built: Vec<(String, clap::Command)> = binaries
        .iter()
        .map(|(n, c)| {
            let mut b = (*c).clone();
            b.build();
            ((*n).to_owned(), b)
        })
        .collect();
    let mut problems = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut checked = 0;
    for line in text.lines() {
        let mut p = line.splitn(4, '\t');
        let (Some(loc), Some(bin), Some(args), Some(src)) =
            (p.next(), p.next(), p.next(), p.next())
        else {
            continue;
        };
        let Some((_, root)) = built.iter().find(|(n, _)| n == bin) else {
            continue;
        };
        let tokens: Vec<String> = args.split('\x1f').map(str::to_owned).collect();
        if tokens.is_empty() || tokens == [""] {
            continue;
        }
        checked += 1;
        if let Some(msg) = walk(&tokens, root) {
            let full = format!("{loc}: {msg}: {}", src.trim());
            if seen.insert(full.clone()) {
                problems.push(full);
            }
        }
    }
    if problems.is_empty() {
        Ok(checked)
    } else {
        Err(format!(
            "{} unknown documented command(s):\n{}",
            problems.len(),
            problems.join("\n")
        ))
    }
}

#[cfg(feature = "test-support")]
fn walk(tokens: &[String], root: &clap::Command) -> Option<String> {
    let mut stack = vec![root];
    let mut trail = vec![root.get_name().to_owned()];
    let mut skip_next = false;
    for token in tokens {
        if skip_next {
            skip_next = false;
            continue;
        }
        if token == "--" || token == "..." || token == "\u{2026}" {
            return None;
        }
        if let Some(name) = token.strip_prefix("--") {
            match check_flag(name, &mut skip_next, &stack) {
                Ok(false) => {}
                Ok(true) => return None,
                Err(msg) => return Some(msg),
            }
            continue;
        }
        if token.starts_with('-') {
            return None;
        }
        let current = stack.last().copied()?;
        if let Some(child) = find_sub(current, token) {
            trail.push(token.clone());
            stack.push(child);
        } else if current.get_subcommands().next().is_some() {
            trail.push(token.clone());
            return Some(format!("unknown command '{}'", trail.join(" ")));
        }
    }
    None
}

#[cfg(feature = "test-support")]
fn check_flag(name: &str, skip_next: &mut bool, stack: &[&clap::Command]) -> Result<bool, String> {
    let flag = name.split('=').next().unwrap_or("");
    if flag == "test" {
        return Ok(true);
    }
    match find_flag(stack, flag) {
        None => Err(format!("unknown flag '--{flag}'")),
        Some(arg) => {
            if takes_value(arg) && !name.contains('=') {
                *skip_next = true;
            }
            Ok(false)
        }
    }
}

#[cfg(feature = "test-support")]
fn find_flag<'t>(stack: &[&'t clap::Command], name: &str) -> Option<&'t clap::Arg> {
    stack
        .iter()
        .rev()
        .find_map(|n| n.get_arguments().find(|a| a.get_long() == Some(name)))
}

#[cfg(feature = "test-support")]
fn takes_value(arg: &clap::Arg) -> bool {
    use clap::ArgAction as A;
    matches!(arg.get_action(), A::Set | A::Append)
        || arg.get_num_args().is_some_and(|c| c.takes_values())
}

#[cfg(feature = "test-support")]
fn find_sub<'t>(node: &'t clap::Command, token: &str) -> Option<&'t clap::Command> {
    node.get_subcommands().find(|c| {
        c.get_name() == token
            || c.get_aliases().any(|a| a == token)
            || c.get_visible_aliases().any(|a| a == token)
    })
}
