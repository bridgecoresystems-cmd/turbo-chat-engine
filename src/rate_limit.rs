use std::time::{Duration, Instant};

/// Token-bucket rate limiter per connection.
/// Allows up to `capacity` messages per second with smooth bursting.
pub struct RateLimiter {
    capacity:   u32,     // max tokens (= max burst)
    tokens:     f64,     // current tokens
    last_refill: Instant,
}

impl RateLimiter {
    pub fn new(msgs_per_sec: u32) -> Self {
        Self {
            capacity:    msgs_per_sec,
            tokens:      msgs_per_sec as f64,
            last_refill: Instant::now(),
        }
    }

    /// Returns true if the message is allowed, false if rate-limited.
    pub fn allow(&mut self) -> bool {
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let now     = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        let new_tokens = elapsed.as_secs_f64() * self.capacity as f64;
        self.tokens = (self.tokens + new_tokens).min(self.capacity as f64);
        self.last_refill = now;
    }
}

/// How long to wait before warning again after a rate-limit hit
pub const WARN_INTERVAL: Duration = Duration::from_secs(5);
