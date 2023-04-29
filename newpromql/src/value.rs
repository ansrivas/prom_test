use itertools::Itertools;
use serde::{
    ser::{SerializeSeq, Serializer},
    Serialize,
};
use std::{cmp::Ordering, collections::HashMap};

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
                extrapolated_sample(first, last, t_start)
            },
            if last.timestamp == t_end {
                *last
            } else {
                extrapolated_sample(first, last, t_end)
            },
        ))
    }
}

// https://promlabs.com/blog/2021/01/29/how-exactly-does-promql-calculate-rates/#extrapolation-of-data
fn extrapolated_sample(p1: &Sample, p2: &Sample, t: i64) -> Sample {
    let dt = p2.timestamp - p1.timestamp;
    let dv = p2.value - p1.value;
    let dt2 = t - p1.timestamp;
    let dv2 = dv * dt2 as f64 / dt as f64;
    Sample {
        timestamp: t,
        value: p1.value + dv2,
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
                v.sort_by(|a, b| {
                    b.value
                        .value
                        .partial_cmp(&a.value.value)
                        .unwrap_or(Ordering::Equal)
                });
            }
            Value::Matrix(v) => {
                v.sort_by(|a, b| {
                    let a = a.values.iter().map(|x| x.value).sum::<f64>();
                    let b = b.values.iter().map(|x| x.value).sum::<f64>();
                    b.partial_cmp(&a).unwrap_or(Ordering::Equal)
                });
            }
            _ => {}
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Signature([u8; 32]);

impl From<Signature> for String {
    fn from(sig: Signature) -> Self {
        hex::encode(sig.0)
    }
}

// REFACTORME: make this a method of `Metric`
pub fn signature(metric: &Metric) -> Signature {
    signature_without_labels(metric, &[])
}

/// `signature_without_labels` is just as [`signature`], but only for labels not matching `names`.
// REFACTORME: make this a method of `Metric`
pub fn signature_without_labels(metric: &Metric, exclude_names: &[&str]) -> Signature {
    let mut hasher = blake3::Hasher::new();
    metric
        .iter()
        .filter(|(k, _)| !exclude_names.contains(&k.as_str()))
        .sorted_unstable_by_key(|(k, _)| k.as_str())
        .for_each(|(k, v)| {
            hasher.update(k.as_bytes());
            hasher.update(v.as_bytes());
        });
    Signature(hasher.finalize().into())
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

        let sig = signature(&metric);
        expect![[r#"
            "f287fde2994111abd7740b5c7c28b0eeabe3f813ae65397bb6acb684e2ab6b22"
        "#]]
        .assert_debug_eq(&String::from(sig));

        let sig: String = signature_without_labels(&metric, &["a", "c"]).into();
        expect![[r#"
            "ec9c3a0c9c03420d330ab62021551cffe993c07b20189c5ed831dad22f54c0c7"
        "#]]
        .assert_debug_eq(&sig);
        assert_eq!(sig.len(), 64);

        assert_eq!(signature(&metric), signature_without_labels(&metric, &[]));
    }

    #[test]
    fn test_extrapolated_sample() {
        let p1 = Sample {
            timestamp: 100,
            value: 10.0,
        };
        let p2 = Sample {
            timestamp: 200,
            value: 20.0,
        };
        let p3 = extrapolated_sample(&p1, &p2, 300);
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
        let p3 = extrapolated_sample(&p1, &p2, 750);
        let p4 = extrapolated_sample(&p1, &p2, 150);
        assert_eq!(format!("{:.2}", p3.value - p4.value), "1.33");

        let p1 = Sample {
            timestamp: 375,
            value: 1.0,
        };
        let p2 = Sample {
            timestamp: 675,
            value: 2.0,
        };
        let p3 = extrapolated_sample(&p1, &p2, 750);
        let p4 = extrapolated_sample(&p1, &p2, 300);
        assert_eq!(format!("{:.2}", p3.value - p4.value), "1.50");
    }
}
