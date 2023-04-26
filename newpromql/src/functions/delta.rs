use datafusion::error::Result;

use crate::value::{RangeValue, Value};

pub(crate) fn delta(data: &Value) -> Result<Value> {
    super::eval_idelta(data, "delta", exec)
}

fn exec(range: &RangeValue) -> f64 {
    range
        .extrapolate()
        .map_or(0.0, |(first, last)| last.value - first.value)
}
