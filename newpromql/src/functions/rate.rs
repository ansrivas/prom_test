use datafusion::error::{DataFusionError, Result};

use crate::value::{InstantValue, Sample, Value};

pub(crate) fn rate(timestamp: i64, data: &Value) -> Result<Value> {
    let data = match data {
        Value::MatrixValues(v) => v,
        _ => {
            return Err(DataFusionError::Internal(
                "rate function only accept vector".to_string(),
            ))
        }
    };

    let rate_values = data
        .iter()
        .map(|metric| {
            let value = rate_exec(&metric.values);
            InstantValue {
                metric: metric.metric.clone(),
                value: Sample { timestamp, value },
            }
        })
        .collect();
    Ok(Value::VectorValues(rate_values))
}

fn rate_exec(data: &[Sample]) -> f64 {
    if data.len() <= 1 {
        return 0.;
    }
    let first = data.first().unwrap();
    let last = data.last().unwrap();
    let dt_seconds = (last.timestamp - first.timestamp) / 1_000_000;
    if dt_seconds == 0 {
        return 0.;
    }
    (last.value - first.value) / dt_seconds as f64
}
