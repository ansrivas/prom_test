use datafusion::error::Result;

use crate::value::{RangeValue, Value};

pub(crate) fn idelta(data: &Value) -> Result<Value> {
    super::eval_idelta(data, "idelta", exec)
}

fn exec(data: &RangeValue) -> f64 {
    if data.values.len() <= 1 {
        return 0.0;
    }
    let (last, data) = data.values.split_last().unwrap();
    let previous = match data.last() {
        Some(v) => v,
        None => return 0.0,
    };
    last.value - previous.value
}
