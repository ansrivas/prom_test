use datafusion::error::Result;
use promql_parser::parser::LabelModifier;

use crate::value::Value;

pub fn max(timestamp: i64, param: &Option<LabelModifier>, data: &Value) -> Result<Value> {
    let score_values =
        super::eval_arithmetic(
            param,
            data,
            "max",
            |prev, val| {
                if prev >= val {
                    prev
                } else {
                    val
                }
            },
        )?;
    if score_values.is_none() {
        return Ok(Value::None);
    }
    let values = score_values
        .unwrap()
        .into_values()
        .map(|x| x.into_instant_value(timestamp, |x| x.value))
        .collect();
    Ok(Value::Vector(values))
}
