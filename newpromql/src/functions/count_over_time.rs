use datafusion::error::Result;

use crate::value::{RangeValue, Value};

pub(crate) fn count_over_time(data: &Value) -> Result<Value> {
    super::eval_idelta(data, "count_over_time", exec)
}

fn exec(data: &RangeValue) -> f64 {
    data.samples.len() as f64
}
