use datafusion::error::Result;

use crate::value::{RangeValue, Value};

pub(crate) fn increase(data: &Value) -> Result<Value> {
    super::eval_idelta(data, "increase", exec)
}

fn exec(range: &RangeValue) -> f64 {
    range
        .extrapolate()
        .map_or(0.0, |(first, last)| last.value - first.value)
}
