use datafusion::error::Result;

use crate::value::{Sample, Value};

pub(crate) fn irate(timestamp: i64, data: &Value) -> Result<Value> {
    super::eval_idelta(timestamp, data, "irate", exec)
}

fn exec(data: &[Sample]) -> f64 {
    if data.len() <= 1 {
        return 0.;
    }
    let (end_value, data) = data.split_last().unwrap();
    let previous_value = match data.last() {
        Some(v) => v,
        None => return 0.,
    };
    let dt_seconds = (end_value.timestamp - previous_value.timestamp) / 1_000_000;
    if dt_seconds == 0 {
        return 0.;
    }
    (end_value.value - previous_value.value) / dt_seconds as f64
}
