use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::labels;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Point {
    /// Time in microseconds
    pub timestamp: i64,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StackValue {
    VectorValue(VectorValue),
    MatrixValue(Vec<VectorValue>),
    MatrixValueResponse(Vec<VectorValueResponse>),
    NumberLiteral(f64),
    None,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorValue {
    pub metric: labels::Signature,
    pub values: HashMap<i64, Vec<Point>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorValueResponse {
    pub metric: labels::Signature,
    pub values: Vec<Point>,
}
