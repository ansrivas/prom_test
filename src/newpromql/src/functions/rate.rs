use chrono::Duration;

use datafusion::error::{DataFusionError, Result};

use crate::{Point, StackValue, VectorValueResponse};

pub(crate) fn rate(data: &StackValue) -> Result<StackValue> {
    let mut rate_value: Vec<VectorValueResponse> = Vec::new();
    let data = match data {
        StackValue::MatrixValue(v) => v,
        _ => {
            return Err(DataFusionError::Internal(
                "rate function only accept vector".to_string(),
            ))
        }
    };

    for metric in data {
        let mut metric_data = VectorValueResponse {
            metric: metric.metric.clone(),
            values: Vec::new(),
        };
        for (t, values) in metric.values.iter() {
            let value = rate_exec(values);
            match value {
                Ok(v) => metric_data.values.push(Point {
                    timestamp: *t,
                    value: v,
                }),
                Err(e) => return Err(e),
            }
        }
        metric_data
            .values
            .sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        rate_value.push(metric_data);
    }

    Ok(StackValue::MatrixValueResponse(rate_value))
}

fn rate_exec(data: &[Point]) -> Result<f64> {
    if data.is_empty() {
        return Ok(0.0);
    }
    let first_value = data.first().unwrap();
    let end_value = data.last().unwrap();
    let value = (end_value.value - first_value.value)
        / ((end_value.timestamp - first_value.timestamp)
            / Duration::seconds(1).num_microseconds().unwrap()) as f64;
    Ok(value)
}
