use datafusion::error::Result;
use promql_parser::parser::LabelModifier;

use crate::value::Value;

pub fn count(timestamp: i64, param: &Option<LabelModifier>, data: &Value) -> Result<Value> {
    let score_values = super::eval_arithmetic(param, data, "count", |_prev, _val| 0.0)?;
    if score_values.is_none() {
        return Ok(Value::None);
    }
    let values = score_values
        .unwrap()
        .into_values()
        .map(|x| x.into_instant_value(timestamp, |x| x.num as f64))
        .collect();
    Ok(Value::Vector(values))
}
