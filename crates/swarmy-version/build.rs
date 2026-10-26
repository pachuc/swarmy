use std::process::Command;

fn git(arguments: &[&str]) -> Option<String> {
    let output = Command::new("git").args(arguments).output().ok()?;
    output.status.success().then_some(())?;
    let value = String::from_utf8(output.stdout).ok()?;
    Some(value.trim().to_owned())
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=SWARMY_RELEASE_BUILD");
    println!("cargo::rustc-check-cfg=cfg(swarmy_release_build)");
    if std::env::var("SWARMY_RELEASE_BUILD").as_deref() == Ok("1") {
        println!("cargo:rustc-cfg=swarmy_release_build");
    }
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
    // A remote node builds from a copy of the client's checkout without .git;
    // the client records the copied commit and the build scripts pass it here.
    println!("cargo:rerun-if-env-changed=SWARMY_GIT_COMMIT");
    // A fixed length: git shortens to the shortest unique prefix, which differs
    // between clones, and the version guard compares identities exactly.
    let commit = std::env::var("SWARMY_GIT_COMMIT")
        .ok()
        .filter(|commit| !commit.is_empty())
        .or_else(|| git(&["rev-parse", "--short=12", "HEAD"]))
        .unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=SWARMY_GIT_COMMIT={commit}");
}
