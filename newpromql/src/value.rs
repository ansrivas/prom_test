use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub static _FIELD_NAME: &str = "__name__";
pub static FIELD_HASH: &str = "__hash__";
pub static FIELD_TYPE: &str = "metric_type";
pub static FIELD_TIME: &str = "_timestamp";
pub static FIELD_VALUE: &str = "value";

type Metric = HashMap<String, String>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Sample {
    /// Time in microseconds
    pub timestamp: i64,
    pub value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstantValue {
    pub metric: Metric,
    pub value: Sample,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeValue {
    pub metric: Metric,
    pub values: Vec<Sample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    InstantValue(InstantValue),
    RangeValue(RangeValue),
    VectorValues(Vec<InstantValue>),
    MatrixValues(Vec<RangeValue>),
    NumberLiteral(f64),
    None,
}

impl Value {
    pub fn is_empty(&self) -> bool {
        match self {
            Value::None => true,
            _ => false,
        }
    }

    pub fn get_ref_matrix_values(&self) -> Option<&Vec<RangeValue>> {
        match self {
            Value::MatrixValues(values) => Some(values),
            _ => None,
        }
    }
}

pub fn signature(data: &Metric) -> String {
    let mut strs = data
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>();
    strs.sort();
    format!("{:x}", md5::compute(strs.join(",")))
}
