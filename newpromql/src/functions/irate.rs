use datafusion::error::{DataFusionError, Result};

use crate::value::{InstantValue, Sample, Value};

pub(crate) fn irate(timestamp: i64, data: &Value) -> Result<Value> {
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
            let value = irate_exec(&metric.values);
            InstantValue {
                metric: metric.metric.clone(),
                value: Sample { timestamp, value },
            }
        })
        .collect();
    Ok(Value::VectorValues(rate_values))
}
fn irate_exec(data: &[Sample]) -> f64 {
    if data.len() <= 1 {
        return 0.;
    }
    let (end_value, data) = data.split_last().unwrap();
    let previous_value = match data.last() {
        Some(v) => v,
        None => return 0.,
    };
    let dt_seconds = (end_value.timestamp - previous_value.timestamp) / 1_000_000;
    if dt_seconds == 0 {
        return 0.;
    }
    (end_value.value - previous_value.value) / dt_seconds as f64
}
