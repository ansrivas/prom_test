use datafusion::error::{DataFusionError, Result};

use crate::value::{RangeValue, Sample, Value};

pub(crate) fn topk(n: usize, data: &Value) -> Result<Value> {
    let mut topk_value: Vec<RangeValue> = Vec::new();
    let data = match data {
        Value::RangeValue(v) => v,
        _ => {
            return Err(DataFusionError::Internal(
                "topk function only accept vector".to_string(),
            ))
        }
    };

    let mut score_value = Vec::new();
    for (i, item) in data.iter().enumerate() {
        score_value.push(Item {
            index: i,
            value: topk_exec(&item.values).unwrap(),
        });
    }
    score_value.sort_by(|a, b| b.value.partial_cmp(&a.value).unwrap());
    let score_value = score_value.iter().take(n).collect::<Vec<&Item>>();
    for item in score_value {
        topk_value.push(data[item.index].clone());
    }

    Ok(Value::MatrixValues(topk_value))
}

fn topk_exec(data: &[Sample]) -> Result<i64> {
    let value: f64 = data.iter().map(|x| x.value).sum();
    Ok(value as i64)
}

struct Item {
    index: usize,
    value: i64,
}
