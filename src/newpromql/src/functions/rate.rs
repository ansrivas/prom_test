use datafusion::error::{DataFusionError, Result};

use crate::value::{Point, StackValue, VectorValueResponse};

pub(crate) fn rate(data: &StackValue) -> Result<StackValue> {
    let data = match data {
        StackValue::MatrixValue(v) => v,
        _ => {
            return Err(DataFusionError::Internal(
                "rate function only accept vector".to_string(),
            ))
        }
    };

    let rate_values = data
        .iter()
        .map(|metric| {
            let mut values = metric
                .values
                .iter()
                .map(|(t, vals)| Point {
                    timestamp: *t,
                    value: rate_exec(vals),
                })
                .collect::<Vec<_>>();
            values.sort_by_key(|x: &Point| x.timestamp);
            VectorValueResponse {
                metric: metric.metric.clone(),
                values,
            }
        })
        .collect();
    Ok(StackValue::MatrixValueResponse(rate_values))
}

fn rate_exec(data: &[Point]) -> f64 {
    if data.is_empty() {
        return 0.;
    }
    let first = data.first().unwrap();
    let last = data.last().unwrap();
    let dt_seconds = (last.timestamp - first.timestamp) / 1_000_000;
    (last.value - first.value) / dt_seconds as f64
}
