use datafusion::error::Result;

use crate::value::{RangeValue, Value};

pub(crate) fn irate(data: &Value) -> Result<Value> {
    super::eval_idelta(data, "irate", exec)
}

fn exec(data: &RangeValue) -> f64 {
    if data.values.len() <= 1 {
        return 0.0;
    }
    let (last, data) = data.values.split_last().unwrap();
    let previous = match data.last() {
        Some(v) => v,
        None => return 0.0,
    };
    let dt_seconds = ((last.timestamp - previous.timestamp) / 1_000_000) as f64;
    if dt_seconds == 0.0 {
        return 0.0;
    }
    (last.value - previous.value) / dt_seconds
}
