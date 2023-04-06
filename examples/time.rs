use chrono::{Duration, TimeZone, Utc};

fn main() {
    let second = Duration::seconds(1);
    let time = Utc
        .datetime_from_str("2000-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ")
        .unwrap();
    println!("second: {}", second.num_microseconds().unwrap());
    println!("time: {}", time.timestamp_micros());
}
