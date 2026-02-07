use std::time::{Duration, Instant};
use std::collections::HashMap;
use log::{info};

/// Simple performance timer for measuring operation durations
#[derive(Debug)]
pub struct Timer {
    start: Instant,
    label: String,
}

impl Timer {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            start: Instant::now(),
            label: label.into(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    pub fn elapsed_ms(&self) -> f64 {
        self.elapsed().as_secs_f64() * 1000.0
    }

    pub fn log(&self) {
        info!("[TIMING] {} took {:.2}ms", self.label, self.elapsed_ms());
    }

    pub fn log_with_rate(&self, count: usize) {
        let ms = self.elapsed_ms();
        let rate = count as f64 / (ms / 1000.0);
        info!(
            "[TIMING] {} took {:.2}ms ({} items, {:.0} items/sec)",
            self.label, ms, count, rate
        );
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        self.log();
    }
}

/// Accumulator for tracking multiple operations
#[derive(Debug, Default)]
pub struct TimingStats {
    operations: HashMap<String, Vec<Duration>>,
}

impl TimingStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, label: impl Into<String>, duration: Duration) {
        self.operations
            .entry(label.into())
            .or_insert_with(Vec::new)
            .push(duration);
    }

    pub fn print_summary(&self) {
        info!("\n========== TIMING SUMMARY ==========");
        let mut ops: Vec<_> = self.operations.iter().collect();
        ops.sort_by_key(|(name, _)| *name);

        for (name, durations) in ops {
            let count = durations.len();
            let total: Duration = durations.iter().sum();
            let avg = total / count as u32;
            let min = durations.iter().min().unwrap();
            let max = durations.iter().max().unwrap();

            info!(
                "{:<30} | count: {:>6} | total: {:>8.2}ms | avg: {:>6.2}ms | min: {:>6.2}ms | max: {:>6.2}ms",
                name,
                count,
                total.as_secs_f64() * 1000.0,
                avg.as_secs_f64() * 1000.0,
                min.as_secs_f64() * 1000.0,
                max.as_secs_f64() * 1000.0
            );
        }
        info!("====================================\n");
    }
}

/// Macro for easy timing with automatic logging
#[macro_export]
macro_rules! time_it {
    ($label:expr, $code:block) => {{
        let _timer = $crate::timing::Timer::new($label);
        $code
    }};
}

/// Macro for timing with rate calculation
#[macro_export]
macro_rules! time_with_count {
    ($label:expr, $count:expr, $code:block) => {{
        let timer = $crate::timing::Timer::new($label);
        let result = $code;
        timer.log_with_rate($count);
        std::mem::forget(timer); // Prevent Drop from logging again
        result
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_timer_basic() {
        let timer = Timer::new("test_operation");
        sleep(Duration::from_millis(10));
        assert!(timer.elapsed_ms() >= 10.0);
    }

    #[test]
    fn test_timing_stats() {
        let mut stats = TimingStats::new();
        stats.record("op1", Duration::from_millis(10));
        stats.record("op1", Duration::from_millis(20));
        stats.record("op2", Duration::from_millis(30));

        stats.print_summary();
    }
}