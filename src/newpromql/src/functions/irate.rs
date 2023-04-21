use chrono::Duration;

use datafusion::error::{DataFusionError, Result};

use crate::value::{Point, StackValue, VectorValueResponse};

pub(crate) fn irate(data: &StackValue) -> Result<StackValue> {
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
            let value = irate_exec(values);
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

fn irate_exec(data: &[Point]) -> Result<f64> {
    if data.is_empty() {
        return Ok(0.0);
    }
    let (end_value, data) = data.split_last().unwrap();
    let previous_value = match data.last() {
        Some(v) => v,
        None => return Ok(0.0),
    };
    let value = (end_value.value - previous_value.value)
        / ((end_value.timestamp - previous_value.timestamp)
            / Duration::seconds(1).num_microseconds().unwrap()) as f64;
    Ok(value)
}
