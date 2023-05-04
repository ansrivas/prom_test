use datafusion::error::Result;

use crate::value::{RangeValue, Value};

pub(crate) fn sum_over_time(data: &Value) -> Result<Value> {
    super::eval_idelta(data, "sum_over_time", exec)
}

fn exec(data: &RangeValue) -> f64 {
    data.samples.iter().map(|s| s.value).sum()
}
