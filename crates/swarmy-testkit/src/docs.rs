//! Docs command walker: check documented invocations against the real clap trees.

/// Check documented invocations against the real clap trees. The Python
/// extractor prints location, binary, `\x1f`-joined args, and source per line.
///
/// # Errors
/// Returns the joined unknown commands and flags when docs name none in the tree.
pub fn check_docs_commands(
    repo_root: &std::path::Path,
    binaries: &[(&str, &clap::Command)],
) -> Result<usize, String> {
    // Docs-command failures are operator-facing text in the String error the check reports.
    // ast-grep-ignore: no-stringified-errors
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
    // Docs-command failures are operator-facing text in the String error the check reports.
    // ast-grep-ignore: no-stringified-errors
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

fn find_flag<'t>(stack: &[&'t clap::Command], name: &str) -> Option<&'t clap::Arg> {
    stack
        .iter()
        .rev()
        .find_map(|n| n.get_arguments().find(|a| a.get_long() == Some(name)))
}

fn takes_value(arg: &clap::Arg) -> bool {
    use clap::ArgAction as A;
    matches!(arg.get_action(), A::Set | A::Append)
        || arg.get_num_args().is_some_and(|c| c.takes_values())
}

fn find_sub<'t>(node: &'t clap::Command, token: &str) -> Option<&'t clap::Command> {
    node.get_subcommands().find(|c| {
        c.get_name() == token
            || c.get_aliases().any(|a| a == token)
            || c.get_visible_aliases().any(|a| a == token)
    })
}
