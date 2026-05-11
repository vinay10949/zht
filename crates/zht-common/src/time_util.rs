//! Time utility functions.
//!
//! Provides high-resolution timing for benchmarking and performance measurement.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Time utility for performance measurement.
pub struct TimeUtil;

impl TimeUtil {
    /// Returns the current time in microseconds since Unix epoch.
    pub fn time_usec() -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64() * 1e6 + d.subsec_nanos() as f64 / 1e3)
            .unwrap_or(0.0)
    }

    /// Returns the current time in milliseconds since Unix epoch.
    pub fn time_msec() -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64() * 1e3 + d.subsec_nanos() as f64 / 1e6)
            .unwrap_or(0.0)
    }

    /// Returns the current time in seconds since Unix epoch.
    pub fn time_sec() -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64() + d.subsec_nanos() as f64 / 1e9)
            .unwrap_or(0.0)
    }

    /// Create a new `Instant` for measuring elapsed time.
    pub fn now() -> Instant {
        Instant::now()
    }

    /// Measure elapsed microseconds from a given `Instant`.
    pub fn elapsed_usec(since: Instant) -> f64 {
        since.elapsed().as_secs_f64() * 1e6
    }

    /// Measure elapsed milliseconds from a given `Instant`.
    pub fn elapsed_msec(since: Instant) -> f64 {
        since.elapsed().as_secs_f64() * 1e3
    }

    /// Measure elapsed seconds from a given `Instant`.
    pub fn elapsed_sec(since: Instant) -> f64 {
        since.elapsed().as_secs_f64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_usec_increases() {
        let t1 = TimeUtil::time_usec();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let t2 = TimeUtil::time_usec();
        assert!(t2 > t1);
    }

    #[test]
    fn test_time_msec_increases() {
        let t1 = TimeUtil::time_msec();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let t2 = TimeUtil::time_msec();
        assert!(t2 > t1);
        assert!(t2 - t1 >= 4.0); // At least ~4ms elapsed
    }

    #[test]
    fn test_time_sec_reasonable() {
        let t = TimeUtil::time_sec();
        // Should be a reasonable Unix timestamp (post-2020)
        assert!(t > 1_500_000_000.0);
    }

    #[test]
    fn test_elapsed() {
        let start = TimeUtil::now();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let ms = TimeUtil::elapsed_msec(start);
        assert!(ms >= 9.0);
        let us = TimeUtil::elapsed_usec(start);
        assert!(us >= 9000.0);
    }
}
