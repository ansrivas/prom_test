use datafusion::error::Result;

use crate::value::{Sample, Value};

pub(crate) fn rate(timestamp: i64, data: &Value) -> Result<Value> {
    super::eval_idelta(timestamp, data, "rate", exec)
}

fn exec(data: &[Sample]) -> f64 {
    if data.len() <= 1 {
        return 0.0;
    }
    let first = data.first().unwrap();
    let last = data.last().unwrap();
    let dt_seconds = (last.timestamp - first.timestamp) / 1_000_000;
    if dt_seconds == 0 {
        return 0.0;
    }
    (last.value - first.value) / dt_seconds as f64
}
