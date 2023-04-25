use datafusion::error::Result;

use crate::value::{Sample, Value};

pub(crate) fn max_over_time(timestamp: i64, data: &Value) -> Result<Value> {
    super::eval_idelta(timestamp, data, "max_over_time", exec)
}

fn exec(data: &[Sample]) -> f64 {
    data.iter()
        .map(|s| s.value)
        .max_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap()
}
