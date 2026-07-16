//! Clock abstraction — injectable UTC time source. Tests use `TestClock` to
//! control time deterministically; production uses `SystemClock`.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

/// Source of UTC time for lease operations. `SystemClock` delegates to
/// `chrono::Utc::now`; `TestClock` is controlled by the test harness.
pub trait Clock: Send + Sync {
    fn now(&self) -> chrono::DateTime<chrono::Utc>;
    fn now_sql(&self) -> String {
        self.now().format("%Y-%m-%d %H:%M:%S").to_string()
    }
    fn expires_sql(&self, secs: u32) -> String {
        (self.now() + chrono::Duration::seconds(secs as i64))
            .format("%Y-%m-%d %H:%M:%S")
            .to_string()
    }
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

/// Monotonic, explicitly advanced clock for fenced lease tests. Atomic so
/// concurrent tests can share a reference without blocking.
///
/// SAFETY: AtomicI64 is Send + Sync; TestClock contains only atomics.
#[allow(unsafe_code)]
unsafe impl Send for TestClock {}
#[allow(unsafe_code)]
unsafe impl Sync for TestClock {}

pub struct TestClock {
    offset_ms: AtomicI64,
}

impl TestClock {
    pub fn new(start: chrono::DateTime<chrono::Utc>) -> Self {
        let base = chrono::Utc::now().timestamp_millis();
        let target = start.timestamp_millis();
        Self {
            offset_ms: AtomicI64::new(target - base),
        }
    }

    /// Advance the clock by `d`, returning the new wall-clock time.
    pub fn advance(&self, d: Duration) -> chrono::DateTime<chrono::Utc> {
        self.offset_ms
            .fetch_add(d.as_millis() as i64, Ordering::SeqCst);
        <Self as Clock>::now(self)
    }

    fn raw_now(&self) -> chrono::DateTime<chrono::Utc> {
        let base = chrono::Utc::now().timestamp_millis();
        let offset = self.offset_ms.load(Ordering::SeqCst);
        chrono::DateTime::from_timestamp_millis(base + offset).unwrap_or_else(chrono::Utc::now)
    }
}

impl Clock for TestClock {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        self.raw_now()
    }
}
