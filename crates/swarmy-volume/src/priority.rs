//! Node processes share admission across every attached volume.
use std::{sync::Mutex, time::Duration};

#[derive(Default)]
struct Activity {
    tools: usize,
    uploads: usize,
}
static ACTIVITY: Mutex<Activity> = Mutex::new(Activity {
    tools: 0,
    uploads: 0,
});
static NEXT: tokio::sync::Mutex<Option<tokio::time::Instant>> = tokio::sync::Mutex::const_new(None);
pub const TOOL_UPLOAD_BYTES_PER_SECOND: u64 = 16 * 1024 * 1024;

/// Keep this guard alive for a tool call. Dropping it also handles cancellation.
pub struct ToolActivity(());
impl ToolActivity {
    #[must_use]
    pub fn begin() -> Self {
        ACTIVITY
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tools += 1;
        Self(())
    }
}
impl Drop for ToolActivity {
    fn drop(&mut self) {
        ACTIVITY
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tools -= 1;
    }
}

#[must_use]
pub fn tool_active() -> bool {
    ACTIVITY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .tools
        != 0
}

pub(crate) struct UploadAdmission {
    pub throttled: bool,
}
impl Drop for UploadAdmission {
    fn drop(&mut self) {
        ACTIVITY
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .uploads -= 1;
    }
}

pub(crate) async fn admit() -> UploadAdmission {
    let admission = loop {
        let admission = {
            let mut activity = ACTIVITY
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let throttled = activity.tools != 0;
            if !throttled || activity.uploads < 4 {
                activity.uploads += 1;
                Some(UploadAdmission { throttled })
            } else {
                None
            }
        };
        if let Some(admission) = admission {
            break admission;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    };
    if admission.throttled {
        // Count existing requests too: after a tool starts, admit nothing until
        // their drain leaves fewer than four. Tools never wait for that drain.
        let mut next = NEXT.lock().await;
        if let Some(deadline) = *next {
            tokio::time::sleep_until(deadline).await;
        }
        *next = Some(tokio::time::Instant::now() + Duration::from_micros(15_625));
        drop(next);
        tokio::task::yield_now().await;
    }
    admission
}
