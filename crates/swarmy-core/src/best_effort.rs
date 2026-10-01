//! Best-effort operations whose failure is safe to ignore.
//!
//! Killing a process that may have exited, removing a file that may be
//! gone, or notifying a receiver that may have hung up are normal outcomes,
//! not errors. Call sites use [`ignore_best_effort`] so the attempt is
//! named and the failure is still visible at `debug`, instead of a bare
//! `let _ =` that hides both.

/// Record a best-effort attempt and log its failure at `debug`.
///
/// `what` names the attempt ("kill child process", "remove pid file") so a
/// debug log explains itself without revisiting the call site.
pub fn ignore_best_effort<T, E: std::fmt::Debug>(result: Result<T, E>, what: &str) {
    if let Err(error) = result {
        // ignore_best_effort logs a generic Debug value that is usually not an error.
        // ast-grep-ignore: no-unchained-error-logs
        tracing::debug!(%what, ?error, "best-effort operation failed");
    }
}
