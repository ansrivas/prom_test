use datafusion::error::Result;

use crate::value::{Sample, Value};

pub(crate) fn increase(timestamp: i64, data: &Value) -> Result<Value> {
    super::eval_idelta(timestamp, data, "increase", exec)
}

fn exec(data: &[Sample]) -> f64 {
    if data.len() <= 1 {
        return 0.0;
    }
    let first = data.first().unwrap();
    let last = data.last().unwrap();
    last.value - first.value
}
