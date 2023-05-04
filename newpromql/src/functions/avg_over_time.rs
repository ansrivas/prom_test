use datafusion::error::Result;

use crate::value::{RangeValue, Value};

pub(crate) fn avg_over_time(data: &Value) -> Result<Value> {
    super::eval_idelta(data, "avg_over_time", exec)
}

fn exec(data: &RangeValue) -> f64 {
    let samples = &data.samples;
    samples.iter().map(|s| s.value).sum::<f64>() / samples.len() as f64
}
