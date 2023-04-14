use datafusion::error::{DataFusionError, Result};

use super::{Point, StackValue, VectorValueResponse};

pub fn topk(n: usize, data: &StackValue) -> Result<StackValue> {
    let mut topk_value: Vec<VectorValueResponse> = Vec::new();
    let data = match data {
        StackValue::MatrixValueResponse(v) => v,
        _ => {
            return Err(DataFusionError::Internal(
                "topk function only accept vector".to_string(),
            ))
        }
    };

    let mut score_value = Vec::new();
    for i in 0..data.len() {
        score_value.push(Item {
            index: i,
            value: topk_exec(&data[i].values).unwrap(),
        });
    }
    score_value.sort_by(|a, b| b.value.partial_cmp(&a.value).unwrap());
    let score_value = score_value.iter().take(n).collect::<Vec<&Item>>();
    for item in score_value {
        topk_value.push(data[item.index].clone());
    }

    Ok(StackValue::MatrixValueResponse(topk_value))
}

fn topk_exec(data: &[Point]) -> Result<i64> {
    let value: f64 = data.iter().map(|x| x.value).sum();
    Ok(value as i64)
}

struct Item {
    index: usize,
    value: i64,
}
