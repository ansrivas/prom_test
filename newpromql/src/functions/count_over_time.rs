use datafusion::error::Result;

use crate::value::{Sample, Value};

pub(crate) fn count_over_time(timestamp: i64, data: &Value) -> Result<Value> {
    super::eval_idelta(timestamp, data, "count_over_time", exec)
}

fn exec(data: &[Sample]) -> f64 {
    data.len() as f64
}
