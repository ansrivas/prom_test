use datafusion::error::{DataFusionError, Result};
use promql_parser::parser::Expr as PromExpr;

use crate::value::{InstantValue, Value};
use crate::QueryEngine;

struct Item {
    index: usize,
    value: f64,
}

pub async fn topk(ctx: &mut QueryEngine, param: Box<PromExpr>, data: &Value) -> Result<Value> {
    let param = ctx.exec_expr(&param).await?;
    let n = match param {
        Value::NumberLiteral(v) => v as usize,
        _ => {
            return Err(DataFusionError::Internal(
                "[topk] param must be NumberLiteral".to_string(),
            ))
        }
    };

    let data = match data {
        Value::VectorValues(v) => v,
        _ => {
            return Err(DataFusionError::Internal(
                "[topk] function only accept vector values".to_string(),
            ))
        }
    };

    let mut score_values = Vec::new();
    for (i, item) in data.iter().enumerate() {
        score_values.push(Item {
            index: i,
            value: item.value.value,
        });
    }
    score_values.sort_by(|a, b| b.value.partial_cmp(&a.value).unwrap());
    let score_values = score_values.iter().take(n).collect::<Vec<&Item>>();

    let mut values: Vec<InstantValue> = Vec::new();
    for item in score_values {
        values.push(data[item.index].clone());
    }
    Ok(Value::VectorValues(values))
}
