use serde::ser::{SerializeSeq, Serializer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const FIELD_HASH: &str = "__hash__";
pub const FIELD_NAME: &str = "__name__";
pub const FIELD_TYPE: &str = "metric_type";
pub const FIELD_TIME: &str = "_timestamp";
pub const FIELD_VALUE: &str = "value";
pub const FIELD_BUCKET: &str = "le";

pub const TYPE_COUNTER: &str = "counter";
pub const TYPE_GAUGE: &str = "gauge";
pub const TYPE_HISTOGRAM: &str = "histogram";
pub const TYPE_SUMMARY: &str = "summary";

pub type Metric = HashMap<String, String>;

#[derive(Debug, Clone, Copy)]
pub struct Sample {
    /// Time in microseconds
    pub timestamp: i64,
    pub value: f64,
}

impl Serialize for Sample {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(&(self.timestamp / 1_000_000))?;
        seq.serialize_element(&self.value.to_string())?;
        seq.end()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct InstantValue {
    pub metric: Metric,
    pub value: Sample,
}

#[derive(Debug, Clone, Serialize)]
pub struct RangeValue {
    pub metric: Metric,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<(i64, i64)>,
    pub values: Vec<Sample>,
}

impl RangeValue {
    /// Returns first and last data points, [extrapolated] to the time window
    /// boundaries.
    ///
    /// [extrapolated]: https://promlabs.com/blog/2021/01/29/how-exactly-does-promql-calculate-rates/#extrapolation-of-data
    pub(crate) fn extrapolate(&self) -> Option<(Sample, Sample)> {
        let samples = &self.values;
        if samples.len() < 2 {
            return None;
        }
        let first = samples.first().unwrap();
        let last = samples.last().unwrap();

        let (t_start, t_end) = self.time.unwrap();
        assert!(t_start < t_end);
        assert!(t_start <= first.timestamp);
        assert!(first.timestamp <= last.timestamp);
        assert!(last.timestamp <= t_end);

        Some((
            if first.timestamp == t_start {
                *first
            } else {
                extrapolate_sample(first, last, t_start)
            },
            if last.timestamp == t_end {
                *last
            } else {
                extrapolate_sample(first, last, t_end)
            },
        ))
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum Value {
    Instant(InstantValue),
    Range(RangeValue),
    Vector(Vec<InstantValue>),
    Matrix(Vec<RangeValue>),
    Sample(Sample), // only used for return literal value
    Float(f64),
    None,
}

impl Value {
    pub(crate) fn get_ref_matrix_values(&self) -> Option<&Vec<RangeValue>> {
        match self {
            Value::Matrix(values) => Some(values),
            _ => None,
        }
    }

    pub fn get_type(&self) -> &str {
        match self {
            Value::Instant(_) => "vector",
            Value::Range(_) => "matrix",
            Value::Vector(_) => "vector",
            Value::Matrix(_) => "matrix",
            Value::Sample(_) => "scalar",
            Value::Float(_) => "scalar",
            Value::None => "scalar",
        }
    }

    pub fn sort(&mut self) {
        match self {
            Value::Vector(v) => {
                v.sort_by(|a, b| b.value.value.partial_cmp(&a.value.value).unwrap());
            }
            Value::Matrix(v) => {
                v.sort_by(|a, b| {
                    let a = a.values.first().unwrap();
                    let b = b.values.first().unwrap();
                    b.value.partial_cmp(&a.value).unwrap()
                });
            }
            _ => {}
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Signature(String);

impl Signature {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub fn signature(metric: &Metric) -> Signature {
    signature_without_labels(metric, &[])
}

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

/// https://promlabs.com/blog/2021/01/29/how-exactly-does-promql-calculate-rates/#extrapolation-of-data
fn extrapolate_sample(p1: &Sample, p2: &Sample, t: i64) -> Sample {
    let dt = p2.timestamp - p1.timestamp;
    let dv = p2.value - p1.value;
    let dt2 = t - p1.timestamp;
    let dv2 = dv * dt2 as f64 / dt as f64;
    Sample {
        timestamp: t,
        value: p1.value + dv2,
    }
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

        assert_eq!(signature(&metric), signature_without_labels(&metric, &[]));
    }

    #[test]
    fn test_extrapolate_sample() {
        let p1 = Sample {
            timestamp: 100,
            value: 10.0,
        };
        let p2 = Sample {
            timestamp: 200,
            value: 20.0,
        };
        let p3 = extrapolate_sample(&p1, &p2, 300);
        assert_eq!(p3.timestamp, 300);
        assert_eq!(p3.value, 30.0);

        let p1 = Sample {
            timestamp: 225,
            value: 1.0,
        };
        let p2 = Sample {
            timestamp: 675,
            value: 2.0,
        };
        let p3 = extrapolate_sample(&p1, &p2, 750);
        let p4 = extrapolate_sample(&p1, &p2, 150);
        assert_eq!(format!("{:.2}", p3.value - p4.value), "1.33");

        let p1 = Sample {
            timestamp: 375,
            value: 1.0,
        };
        let p2 = Sample {
            timestamp: 675,
            value: 2.0,
        };
        let p3 = extrapolate_sample(&p1, &p2, 750);
        let p4 = extrapolate_sample(&p1, &p2, 300);
        assert_eq!(format!("{:.2}", p3.value - p4.value), "1.50");
    }
}
