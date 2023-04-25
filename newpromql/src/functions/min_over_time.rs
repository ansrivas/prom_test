use datafusion::error::Result;

use crate::value::{Sample, Value};

pub(crate) fn min_over_time(timestamp: i64, data: &Value) -> Result<Value> {
    super::eval_idelta(timestamp, data, "min_over_time", exec)
}

fn exec(data: &[Sample]) -> f64 {
    data.iter()
        .map(|s| s.value)
        .min_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap()
}
