use datafusion::error::Result;

use crate::value::{extrapolate_sample, RangeValue, Value};

pub(crate) fn rate(data: &Value) -> Result<Value> {
    super::eval_idelta(data, "rate", exec)
}

fn exec(data: &RangeValue) -> f64 {
    if data.values.len() <= 1 {
        return 0.0;
    }
    let first = data.values.first().unwrap();
    let last = data.values.last().unwrap();

    let d_first = if first.timestamp != data.time.unwrap().0 {
        extrapolate_sample(first, last, data.time.unwrap().0)
    } else {
        *first
    };
    let d_last = if last.timestamp != data.time.unwrap().1 {
        extrapolate_sample(first, last, data.time.unwrap().1)
    } else {
        *last
    };
    let dt_seconds = ((d_last.timestamp - d_first.timestamp) / 1_000_000) as f64;
    if dt_seconds == 0.0 {
        return 0.0;
    }
    (d_last.value - d_first.value) / dt_seconds
}
