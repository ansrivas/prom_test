use datafusion::error::Result;
use promql_parser::parser::LabelModifier;

use crate::value::{InstantValue, Sample, Value};

pub fn min(timestamp: i64, param: &Option<LabelModifier>, data: &Value) -> Result<Value> {
    let score_values = super::eval_arithmetic(param, data, "min", |prev, val| {
        if prev > 0.0 && prev <= val {
            prev
        } else {
            val
        }
    })?;
    if score_values.is_none() {
        return Ok(Value::None);
    }
    let values = score_values
        .unwrap()
        .values()
        .map(|v| InstantValue {
            labels: v.labels.clone(),
            value: Sample {
                timestamp,
                value: v.value,
            },
        })
        .collect::<Vec<_>>();
    Ok(Value::Vector(values))
}
