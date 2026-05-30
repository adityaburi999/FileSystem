use std::env;

pub struct FuseDemoConfig {
    pub wal_sync_writes: bool,
    pub chunk_size_bytes: usize,
    pub daemon_request_timeout_ms: Option<u64>,
    pub daemon_max_pending_requests: usize,
    pub timeout_smoke: bool,
    pub timeout_smoke_expect_timeout: bool,
    pub timeout_smoke_delay_ms: u64,
}

impl FuseDemoConfig {
    pub fn from_env(default_chunk_size_bytes: usize) -> Self {
        Self {
            wal_sync_writes: parse_bool_env("WAL_SYNC_WRITES", true),
            chunk_size_bytes: parse_usize_env("CHUNK_SIZE_BYTES", default_chunk_size_bytes, 1),
            daemon_request_timeout_ms: parse_optional_u64_env("FUSE_DAEMON_REQUEST_TIMEOUT_MS"),
            daemon_max_pending_requests: parse_usize_env(
                "FUSE_DAEMON_MAX_PENDING_REQUESTS",
                1024,
                1,
            ),
            timeout_smoke: parse_bool_env("FUSE_TIMEOUT_SMOKE", false),
            timeout_smoke_expect_timeout: parse_bool_env(
                "FUSE_TIMEOUT_SMOKE_EXPECT_TIMEOUT",
                false,
            ),
            timeout_smoke_delay_ms: parse_u64_env("FUSE_TIMEOUT_SMOKE_DELAY_MS", 80),
        }
    }

    pub fn summary(&self, demo_name: &str) -> String {
        format!(
            "{demo_name} config: WAL_SYNC_WRITES={}, CHUNK_SIZE_BYTES={}, FUSE_DAEMON_REQUEST_TIMEOUT_MS={}, FUSE_DAEMON_MAX_PENDING_REQUESTS={}, FUSE_TIMEOUT_SMOKE={}, FUSE_TIMEOUT_SMOKE_EXPECT_TIMEOUT={}, FUSE_TIMEOUT_SMOKE_DELAY_MS={}",
            self.wal_sync_writes,
            self.chunk_size_bytes,
            self.daemon_request_timeout_ms
                .map(|v| v.to_string())
                .unwrap_or_else(|| "none".to_string()),
            self.daemon_max_pending_requests,
            self.timeout_smoke,
            self.timeout_smoke_expect_timeout,
            self.timeout_smoke_delay_ms
        )
    }

    pub fn validate_for_daemon_smoke(&self) -> Result<(), String> {
        if self.timeout_smoke_expect_timeout && !self.timeout_smoke {
            return Err(
                "FUSE_TIMEOUT_SMOKE_EXPECT_TIMEOUT requires FUSE_TIMEOUT_SMOKE=true".to_string(),
            );
        }
        if self.timeout_smoke_expect_timeout && self.daemon_request_timeout_ms.is_none() {
            return Err("FUSE_TIMEOUT_SMOKE_EXPECT_TIMEOUT requires FUSE_DAEMON_REQUEST_TIMEOUT_MS to be set".to_string());
        }
        if self.timeout_smoke_expect_timeout {
            let timeout_ms = self.daemon_request_timeout_ms.unwrap_or_default();
            if timeout_ms >= self.timeout_smoke_delay_ms {
                return Err(format!(
                    "timeout smoke expects timeout, but configured timeout ({timeout_ms}ms) is not less than delay ({}ms)",
                    self.timeout_smoke_delay_ms
                ));
            }
        }
        Ok(())
    }
}

fn parse_bool_env(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(raw) => parse_bool_text(&raw),
        Err(_) => default,
    }
}

fn parse_usize_env(key: &str, default: usize, min: usize) -> usize {
    match env::var(key) {
        Ok(raw) => raw
            .trim()
            .parse::<usize>()
            .ok()
            .map(|v| v.max(min))
            .unwrap_or(default.max(min)),
        Err(_) => default.max(min),
    }
}

fn parse_optional_u64_env(key: &str) -> Option<u64> {
    match env::var(key) {
        Ok(raw) => parse_optional_u64_text(&raw),
        Err(_) => None,
    }
}

fn parse_u64_env(key: &str, default: u64) -> u64 {
    match env::var(key) {
        Ok(raw) => raw.trim().parse::<u64>().unwrap_or(default),
        Err(_) => default,
    }
}

fn parse_bool_text(raw: &str) -> bool {
    let normalized = raw.trim().to_ascii_lowercase();
    !matches!(normalized.as_str(), "0" | "false" | "off" | "no")
}

fn parse_optional_u64_text(raw: &str) -> Option<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if matches!(trimmed.to_ascii_lowercase().as_str(), "none" | "off") {
        return None;
    }
    trimmed.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bool_text_falsey_values() {
        assert!(!parse_bool_text("0"));
        assert!(!parse_bool_text("false"));
        assert!(!parse_bool_text("off"));
        assert!(!parse_bool_text("No"));
    }

    #[test]
    fn parse_bool_text_truthy_values() {
        assert!(parse_bool_text("1"));
        assert!(parse_bool_text("true"));
        assert!(parse_bool_text("on"));
        assert!(parse_bool_text("yes"));
    }

    #[test]
    fn parse_optional_u64_text_none_and_valid() {
        assert_eq!(parse_optional_u64_text(""), None);
        assert_eq!(parse_optional_u64_text("none"), None);
        assert_eq!(parse_optional_u64_text("off"), None);
        assert_eq!(parse_optional_u64_text("25"), Some(25));
        assert_eq!(parse_optional_u64_text("oops"), None);
    }

    #[test]
    fn daemon_smoke_validation_rejects_missing_timeout_when_expecting_timeout() {
        let config = FuseDemoConfig {
            wal_sync_writes: true,
            chunk_size_bytes: 4,
            daemon_request_timeout_ms: None,
            daemon_max_pending_requests: 64,
            timeout_smoke: true,
            timeout_smoke_expect_timeout: true,
            timeout_smoke_delay_ms: 80,
        };
        let err = config
            .validate_for_daemon_smoke()
            .expect_err("validation should fail");
        assert!(err.contains("FUSE_DAEMON_REQUEST_TIMEOUT_MS"));
    }

    #[test]
    fn daemon_smoke_validation_rejects_non_strict_timeout_window() {
        let config = FuseDemoConfig {
            wal_sync_writes: true,
            chunk_size_bytes: 4,
            daemon_request_timeout_ms: Some(80),
            daemon_max_pending_requests: 64,
            timeout_smoke: true,
            timeout_smoke_expect_timeout: true,
            timeout_smoke_delay_ms: 80,
        };
        let err = config
            .validate_for_daemon_smoke()
            .expect_err("validation should fail");
        assert!(err.contains("not less than delay"));
    }

    #[test]
    fn daemon_smoke_validation_accepts_strict_timeout_window() {
        let config = FuseDemoConfig {
            wal_sync_writes: true,
            chunk_size_bytes: 4,
            daemon_request_timeout_ms: Some(25),
            daemon_max_pending_requests: 64,
            timeout_smoke: true,
            timeout_smoke_expect_timeout: true,
            timeout_smoke_delay_ms: 80,
        };
        config
            .validate_for_daemon_smoke()
            .expect("validation should pass");
    }
}
