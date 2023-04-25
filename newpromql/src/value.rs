use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const FIELD_HASH: &str = "__hash__";
pub const FIELD_NAME: &str = "__name__";
pub const FIELD_TYPE: &str = "metric_type";
pub const FIELD_TIME: &str = "_timestamp";
pub const FIELD_VALUE: &str = "value";
pub const FIELD_BUCKET: &str = "le";

pub type Metric = HashMap<String, String>;

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

/// A simple numeric floating point value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalarValue {
    pub metric: Metric,
    pub value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    InstantValue(InstantValue),
    RangeValue(RangeValue),
    VectorValues(Vec<InstantValue>),
    MatrixValues(Vec<RangeValue>),
    NumberLiteral(f64),
    ScalarValues(Vec<ScalarValue>),
    None,
}

impl Value {
    pub fn is_none(&self) -> bool {
        matches!(self, Value::None)
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Signature(String);

// `signature_without_labels` is just as [`signature`], but only for labels not matching `names`.
pub fn signature_without_labels(metric: &Metric, names: &[&str]) -> Signature {
    Signature(format!(
        "{:x}",
        md5::compute(sig_without_labels(metric, names))
    ))
}

fn sig_without_labels(metric: &Metric, names: &[&str]) -> String {
    let mut kv_pairs = metric
        .iter()
        .filter_map(|(k, v)| (!names.contains(&k.as_str())).then(|| format!("{k}={v}")))
        .collect::<Vec<_>>();
    kv_pairs.sort();
    kv_pairs.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use expect_test::expect;

    #[test]
    fn test_signature_without_labels() {
        let metric = Metric::from([
            ("a".to_owned(), "1".to_owned()),
            ("b".to_owned(), "2".to_owned()),
            ("c".to_owned(), "3".to_owned()),
            ("d".to_owned(), "4".to_owned()),
        ]);
        expect![[r#"
            "b=2,d=4"
        "#]]
        .assert_debug_eq(&sig_without_labels(&metric, &["a", "c"]));
        expect![[r#"
            Signature(
                "6cb330d225f4df0fefef924f9711ae8f",
            )
        "#]]
        .assert_debug_eq(&signature_without_labels(&metric, &["a", "c"]));
    }
}
