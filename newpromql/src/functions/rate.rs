use datafusion::error::Result;

use crate::value::{RangeValue, Value};

pub(crate) fn rate(data: &Value) -> Result<Value> {
    super::eval_idelta(data, "rate", exec)
}

fn exec(range: &RangeValue) -> f64 {
    range.extrapolate().map_or(0.0, |(first, last)| {
        let dt_seconds = ((last.timestamp - first.timestamp) / 1_000_000) as f64;
        if dt_seconds == 0.0 {
            0.0
        } else {
            (last.value - first.value) / dt_seconds
        }
    })
}
