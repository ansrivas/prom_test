use datafusion::error::Result;

use crate::value::{Sample, Value};

pub(crate) fn avg_over_time(timestamp: i64, data: &Value) -> Result<Value> {
    super::eval_idelta(timestamp, data, "avg_over_time", exec)
}

fn exec(data: &[Sample]) -> f64 {
    data.iter().map(|s| s.value).sum::<f64>() / data.len() as f64
}
