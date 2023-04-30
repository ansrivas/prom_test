use datafusion::error::{DataFusionError, Result};
use promql_parser::parser::Expr as PromExpr;
use promql_parser::parser::LabelModifier;
use rustc_hash::FxHashMap;

use crate::value::{signature, Signature, Value};
use crate::QueryEngine;

mod avg;
mod bottomk;
mod count;
mod max;
mod min;
mod sum;
mod topk;

pub(crate) use avg::avg;
pub(crate) use bottomk::bottomk;
pub(crate) use count::count;
pub(crate) use max::max;
pub(crate) use min::min;
pub(crate) use sum::sum;
pub(crate) use topk::topk;

pub(crate) struct ArithmeticItem {
    pub(crate) labels: FxHashMap<String, String>,
    pub(crate) value: f64,
    pub(crate) num: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct TopItem {
    pub(crate) index: usize,
    pub(crate) value: f64,
}

pub(crate) fn eval_arithmetic(
    param: &Option<LabelModifier>,
    data: &Value,
    f_name: &str,
    f_handler: fn(total: f64, val: f64) -> f64,
) -> Result<Option<FxHashMap<Signature, ArithmeticItem>>> {
    let data = match data {
        Value::Vector(v) => v,
        Value::None => return Ok(None),
        _ => {
            return Err(DataFusionError::Internal(format!(
                "[{f_name}] function only accept vector values"
            )))
        }
    };

    let mut score_values = FxHashMap::default();
    match param {
        Some(v) => match v {
            LabelModifier::Include(labels) => {
                for item in data.iter() {
                    let mut sum_labels = FxHashMap::default();
                    for label in labels {
                        let value = match item.metric.get(label) {
                            Some(v) => v,
                            None => {
                                return Err(DataFusionError::Internal(format!(
                                    "label not found: {label}"
                                )))
                            }
                        };
                        sum_labels.insert(label.clone(), value.clone());
                    }
                    let sum_hash = signature(&sum_labels);
                    let entry = score_values.entry(sum_hash).or_insert(ArithmeticItem {
                        labels: sum_labels,
                        value: 0.0,
                        num: 0,
                    });
                    entry.value = f_handler(entry.value, item.value.value);
                    entry.num += 1;
                }
            }
            LabelModifier::Exclude(labels) => {
                for item in data.iter() {
                    let mut sum_labels = FxHashMap::default();
                    for (label, value) in item.metric.iter() {
                        if !labels.contains(label) {
                            sum_labels.insert(label.clone(), value.clone());
                        }
                    }
                    let sum_hash = signature(&sum_labels);
                    let entry = score_values.entry(sum_hash).or_insert(ArithmeticItem {
                        labels: sum_labels,
                        value: 0.0,
                        num: 0,
                    });
                    entry.value = f_handler(entry.value, item.value.value);
                    entry.num += 1;
                }
            }
        },
        None => {
            for item in data.iter() {
                let entry = score_values
                    .entry(Signature::default())
                    .or_insert(ArithmeticItem {
                        labels: FxHashMap::default(),
                        value: 0.0,
                        num: 0,
                    });
                entry.value = f_handler(entry.value, item.value.value);
                entry.num += 1;
            }
        }
    }
    Ok(Some(score_values))
}

pub async fn eval_top(
    ctx: &mut QueryEngine,
    param: Box<PromExpr>,
    data: &Value,
    is_bottom: bool,
) -> Result<Value> {
    let fn_name = if is_bottom { "bottomk" } else { "topk" };

    let param = ctx.exec_expr(&param).await?;
    let n = match param {
        Value::Float(v) => v as usize,
        _ => {
            return Err(DataFusionError::Internal(format!(
                "[{fn_name}] param must be NumberLiteral"
            )))
        }
    };

    let data = match data {
        Value::Vector(v) => v,
        Value::None => return Ok(Value::None),
        _ => {
            return Err(DataFusionError::Internal(format!(
                "[{fn_name}] function only accept vector values"
            )))
        }
    };

    let mut score_values = Vec::new();
    for (i, item) in data.iter().enumerate() {
        if item.value.value.is_nan() {
            continue;
        }
        score_values.push(TopItem {
            index: i,
            value: item.value.value,
        });
    }

    if is_bottom {
        score_values.sort_by(|a, b| a.value.partial_cmp(&b.value).unwrap());
    } else {
        score_values.sort_by(|a, b| b.value.partial_cmp(&a.value).unwrap());
    }
    let values = score_values
        .iter()
        .take(n)
        .map(|v| data[v.index].clone())
        .collect();
    Ok(Value::Vector(values))
}
