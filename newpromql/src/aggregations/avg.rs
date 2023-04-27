use datafusion::error::Result;
use promql_parser::parser::AggModifier;

use crate::value::{InstantValue, Sample, Value};

pub fn avg(timestamp: i64, param: &Option<AggModifier>, data: &Value) -> Result<Value> {
    let score_values = super::eval_arithmetic(param, data, "avg", |total, val| total + val)?;
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
                value: v.value / v.num as f64,
            },
        })
        .collect::<Vec<_>>();
    Ok(Value::Vector(values))
}
