use std::process::Command;

fn git(arguments: &[&str]) -> Option<String> {
    let output = Command::new("git").args(arguments).output().ok()?;
    output.status.success().then_some(())?;
    let value = String::from_utf8(output.stdout).ok()?;
    Some(value.trim().to_owned())
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    // HEAD can be symbolic, detached, or in a worktree. Watch both it and its
    // resolved ref so an incremental build cannot keep the previous identity.
    for name in ["HEAD", "packed-refs"] {
        if let Some(path) = git(&["rev-parse", "--git-path", name])
            && std::path::Path::new(&path).exists()
        {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    if let Some(reference) = git(&["symbolic-ref", "-q", "HEAD"])
        && let Some(path) = git(&["rev-parse", "--git-path", &reference])
    {
        println!("cargo:rerun-if-changed={path}");
    }
    let commit = git(&["rev-parse", "--short", "HEAD"]).unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=SWARMY_GIT_COMMIT={commit}");
}
