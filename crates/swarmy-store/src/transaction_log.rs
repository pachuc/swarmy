//! Timing for store transactions. Every transaction carries the source
//! location of the code that ran it, so a slow or retried transaction names
//! its caller in the log. The latency suite parses these lines: keep the
//! target, the messages, and the field names stable.
use std::{
    panic::Location,
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, Instant},
};

/// Transactions at or above this duration log at warn; all others at debug.
pub(crate) const SLOW_TRANSACTION: Duration = Duration::from_secs(1);

/// Times one logical transaction across all of its attempts.
pub(crate) struct TransactionTimer {
    caller: &'static Location<'static>,
    started: Instant,
    attempts: AtomicU32,
}

impl TransactionTimer {
    pub(crate) fn start(caller: &'static Location<'static>) -> Self {
        Self {
            caller,
            started: Instant::now(),
            attempts: AtomicU32::new(0),
        }
    }

    /// Count one run of the transaction body. The binding runs the body again
    /// after every retryable read or commit error.
    pub(crate) fn attempt(&self) {
        self.attempts.fetch_add(1, Ordering::Relaxed);
    }

    /// Log the finished transaction with its caller, attempts and elapsed time.
    pub(crate) fn finish(&self, ok: bool) {
        record(
            self.caller,
            self.attempts.load(Ordering::Relaxed),
            self.started.elapsed(),
            ok,
        );
    }
}

fn record(caller: &Location<'_>, attempts: u32, elapsed: Duration, ok: bool) {
    let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    if elapsed >= SLOW_TRANSACTION {
        tracing::warn!(
            target: "swarmy_store::transaction",
            caller = %caller,
            attempts,
            elapsed_ms,
            ok,
            "slow store transaction"
        );
    } else {
        tracing::debug!(
            target: "swarmy_store::transaction",
            caller = %caller,
            attempts,
            elapsed_ms,
            ok,
            "store transaction"
        );
    }
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for SharedBuffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .map_err(|_| std::io::Error::other("log buffer poisoned"))?
                .write(bytes)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for SharedBuffer {
        type Writer = Self;
        fn make_writer(&'writer self) -> Self::Writer {
            self.clone()
        }
    }

    /// Run `log` under a plain-text subscriber like the services use and return its output.
    fn captured(log: impl FnOnce()) -> String {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(SharedBuffer(buffer.clone()))
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .finish();
        tracing::subscriber::with_default(subscriber, log);
        let bytes = buffer.lock().map(|guard| guard.clone()).unwrap_or_default();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn retried_transaction_logs_its_caller_and_attempt_count() {
        let caller = Location::caller();
        let timer = TransactionTimer::start(caller);
        timer.attempt();
        timer.attempt();
        let logged = captured(|| timer.finish(true));
        assert!(logged.contains("swarmy_store::transaction"), "{logged}");
        assert!(logged.contains(&format!("caller={caller}")), "{logged}");
        assert!(logged.contains("attempts=2"), "{logged}");
        assert!(logged.contains("ok=true"), "{logged}");
        assert!(logged.contains("DEBUG"), "{logged}");
    }

    #[test]
    fn transaction_at_the_threshold_logs_a_warning_with_its_timing() {
        let logged = captured(|| record(Location::caller(), 3, SLOW_TRANSACTION, false));
        assert!(logged.contains("WARN"), "{logged}");
        assert!(logged.contains("slow store transaction"), "{logged}");
        assert!(logged.contains("attempts=3"), "{logged}");
        assert!(logged.contains("elapsed_ms=1000"), "{logged}");
        assert!(logged.contains("ok=false"), "{logged}");
    }

    #[test]
    fn fast_transaction_logs_only_at_debug() {
        let logged = captured(|| record(Location::caller(), 1, Duration::from_millis(10), true));
        assert!(logged.contains("DEBUG"), "{logged}");
        assert!(!logged.contains("WARN"), "{logged}");
    }
}
