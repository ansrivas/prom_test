use datafusion::error::Result;

use crate::value::{RangeValue, Value};

pub(crate) fn idelta(data: &Value) -> Result<Value> {
    super::eval_idelta(data, "idelta", exec)
}

fn exec(data: &RangeValue) -> f64 {
    if data.samples.len() < 2 {
        return 0.0;
    }
    let (last, rest) = data.samples.split_last().unwrap();
    rest.last()
        .map_or(0.0, |before_last| last.value - before_last.value)
}
