use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use promql_parser::parser::AggModifier;

use crate::value::{signature, InstantValue, Sample, Value};

struct Item {
    labels: HashMap<String, String>,
    value: f64,
}

pub fn sum(timestamp: i64, param: &Option<AggModifier>, data: &Value) -> Result<Value> {
    let data = match data {
        Value::VectorValues(v) => v,
        _ => {
            return Err(DataFusionError::Internal(
                "[sum] function only accept vector values".to_string(),
            ))
        }
    };

    let mut score_values = HashMap::new();
    match param {
        Some(v) => match v {
            AggModifier::By(labels) => {
                for item in data.iter() {
                    let mut sum_labels = HashMap::new();
                    for label in labels {
                        sum_labels.insert(label.clone(), item.metric.get(label).unwrap().clone());
                    }
                    let sum_hash = signature(&sum_labels);
                    let entry = score_values.entry(sum_hash).or_insert(Item {
                        labels: sum_labels,
                        value: 0.0,
                    });
                    entry.value += item.value.value;
                }
            }
            AggModifier::Without(labels) => {
                for item in data.iter() {
                    let mut sum_labels = HashMap::new();
                    for (label, value) in item.metric.iter() {
                        if !labels.contains(label) {
                            sum_labels.insert(label.clone(), value.clone());
                        }
                    }
                    let sum_hash = signature(&sum_labels);
                    let entry = score_values.entry(sum_hash).or_insert(Item {
                        labels: sum_labels,
                        value: 0.0,
                    });
                    entry.value += item.value.value;
                }
            }
        },
        None => {
            for item in data.iter() {
                let entry = score_values.entry("".to_string()).or_insert(Item {
                    labels: HashMap::new(),
                    value: 0.0,
                });
                entry.value += item.value.value;
            }
        }
    }

    let mut values: Vec<InstantValue> = Vec::new();
    for item in score_values.values() {
        values.push(InstantValue {
            metric: item.labels.clone(),
            value: Sample {
                timestamp,
                value: item.value,
            },
        });
    }
    Ok(Value::VectorValues(values))
}
