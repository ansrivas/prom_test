use datafusion::error::Result;

use crate::value::{RangeValue, Value};

pub(crate) fn min_over_time(data: &Value) -> Result<Value> {
    super::eval_idelta(data, "min_over_time", exec)
}

fn exec(data: &RangeValue) -> f64 {
    if data.samples.is_empty() {
        return 0.0;
    }
    data.samples
        .iter()
        .map(|s| s.value)
        .min_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap()
}
