//! Webhook signing + retry schedule (FR-5.1, FR-5.2). Pure functions, unit-testable.

use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Hard retry ceiling before a webhook log is flagged FAILED (FR-5.2).
pub const MAX_ATTEMPTS: i32 = 5;

/// HMAC-SHA256 over the request body, hex-encoded. Sent as `X-Loyalty-Signature` so receivers
/// can verify authenticity with their per-subscription shared secret (§9).
pub fn sign(secret: &str, body: &str) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(body.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Exponential backoff (seconds) before the next attempt, given how many attempts already failed.
/// 5, 10, 20, 40, … capped at one hour.
pub fn next_backoff_secs(attempts_made: i32) -> i64 {
    let factor = 2_i64.saturating_pow(attempts_made.max(0) as u32);
    factor.saturating_mul(5).min(3600)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_is_deterministic_and_keyed() {
        let a = sign("secret", "body");
        assert_eq!(a, sign("secret", "body"));
        assert_ne!(a, sign("other", "body"));
        assert_eq!(a.len(), 64); // 32 bytes hex
    }

    #[test]
    fn backoff_grows_and_caps() {
        assert_eq!(next_backoff_secs(0), 5);
        assert_eq!(next_backoff_secs(1), 10);
        assert_eq!(next_backoff_secs(3), 40);
        assert_eq!(next_backoff_secs(100), 3600); // capped
    }
}
