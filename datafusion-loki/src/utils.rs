use std::time::{SystemTime, UNIX_EPOCH};

pub fn current_timestamp_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_nanos() as i64
}

pub fn thirty_days_before_now_timestamp_ns() -> i64 {
    current_timestamp_ns() - (30 * 24 * 60 * 60 * 1000 * 1000 * 1000)
}
