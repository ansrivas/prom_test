use datafusion::error::{DataFusionError, Result};
use promql_parser::parser::{Expr as PromExpr, LabelModifier};
use rustc_hash::FxHashMap;

use crate::{
    labels::{self, Labels},
    value::{InstantValue, Sample, Value},
    QueryEngine,
};

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

#[derive(Debug, Clone, Default)]
pub(crate) struct ArithmeticItem {
    pub(crate) labels: Labels,
    pub(crate) value: f64,
    pub(crate) num: usize,
}

impl ArithmeticItem {
    pub(crate) fn into_instant_value(
        self,
        timestamp: i64,
        calculate_sample_value: fn(&Self) -> f64,
    ) -> InstantValue {
        let value = calculate_sample_value(&self);
        InstantValue {
            metric: self.labels,
            sample: Sample { timestamp, value },
        }
    }
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
) -> Result<Option<FxHashMap<labels::Signature, ArithmeticItem>>> {
    let data = match data {
        Value::Vector(v) => v,
        Value::None => return Ok(None),
        _ => {
            return Err(DataFusionError::Internal(format!(
                "{f_name}: vector argument expected"
            )))
        }
    };

    let mut score_values = FxHashMap::default();
    match param {
        Some(v) => match v {
            LabelModifier::Include(labels_to_include) => {
                for ival in data {
                    let mut labels = ival.metric.clone();
                    labels.retain(|label| labels_to_include.contains(&label.name));
                    let entry = score_values
                        .entry(labels.signature())
                        .or_insert(ArithmeticItem {
                            labels,
                            value: 0.0,
                            num: 0,
                        });
                    entry.value = f_handler(entry.value, ival.sample.value);
                    entry.num += 1;
                }
            }
            LabelModifier::Exclude(labels_to_exclude) => {
                for ival in data {
                    let mut labels = ival.metric.clone();
                    labels.retain(|label| !labels_to_exclude.contains(&label.name));
                    let entry = score_values
                        .entry(labels.signature())
                        .or_insert(ArithmeticItem {
                            labels,
                            value: 0.0,
                            num: 0,
                        });
                    entry.value = f_handler(entry.value, ival.sample.value);
                    entry.num += 1;
                }
            }
        },
        None => {
            for ival in data.iter() {
                let entry = score_values
                    .entry(labels::Signature::default())
                    .or_default();
                entry.value = f_handler(entry.value, ival.sample.value);
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
                "{fn_name}: param must be NumberLiteral"
            )))
        }
    };

    let data = match data {
        Value::Vector(v) => v,
        Value::None => return Ok(Value::None),
        _ => {
            return Err(DataFusionError::Internal(format!(
                "{fn_name}: vector argument expected"
            )))
        }
    };

    let mut score_values = data
        .iter()
        .enumerate()
        .filter_map(|(i, ival)| {
            (!ival.sample.value.is_nan()).then_some(TopItem {
                index: i,
                value: ival.sample.value,
            })
        })
        .collect::<Vec<_>>();

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
