use datafusion::error::{DataFusionError, Result};

use crate::value::{InstantValue, Value};

pub(crate) fn topk(n: usize, data: &Value) -> Result<Value> {
    let mut topk_value: Vec<InstantValue> = Vec::new();
    let data = match data {
        Value::VectorValues(v) => v,
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
            value: item.value.value,
        });
    }
    score_value.sort_by(|a, b| b.value.partial_cmp(&a.value).unwrap());
    let score_value = score_value.iter().take(n).collect::<Vec<&Item>>();
    for item in score_value {
        topk_value.push(data[item.index].clone());
    }

    Ok(Value::VectorValues(topk_value))
}

struct Item {
    index: usize,
    value: f64,
}
