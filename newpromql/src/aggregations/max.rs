use datafusion::error::Result;
use promql_parser::parser::AggModifier;

use crate::value::{InstantValue, Sample, Value};

pub fn max(timestamp: i64, param: &Option<AggModifier>, data: &Value) -> Result<Value> {
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
        .values()
        .map(|v| InstantValue {
            metric: v.labels.clone(),
            value: Sample {
                timestamp,
                value: v.value,
            },
        })
        .collect::<Vec<_>>();
    Ok(Value::VectorValues(values))
}
