use datafusion::error::Result;

use crate::value::{Sample, Value};

pub(crate) fn idelta(timestamp: i64, data: &Value) -> Result<Value> {
    super::eval_idelta(timestamp, data, "idelta", exec)
}

fn exec(data: &[Sample]) -> f64 {
    if data.len() <= 1 {
        return 0.0;
    }
    let (last, data) = data.split_last().unwrap();
    let previous = match data.last() {
        Some(v) => v,
        None => return 0.0,
    };
    last.value - previous.value
}
