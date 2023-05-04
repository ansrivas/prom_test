use std::cmp::Ordering;

use serde::{
    ser::{SerializeSeq, SerializeStruct, Serializer},
    Serialize,
};

use crate::labels::Labels;

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

/// Time series — a stream of data points belonging to a metric.
///
/// See <https://promlabs.com/blog/2020/07/02/selecting-data-in-promql/#refresher-prometheus-data-model>
#[derive(Debug, Clone)]
pub(crate) struct Series {
    pub(crate) metric: Labels,
    pub(crate) samples: Vec<Sample>,
}

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

#[derive(Debug, Clone)]
pub struct InstantValue {
    pub metric: Labels,
    pub sample: Sample,
}

impl Serialize for InstantValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("InstantValue", 2)?;
        state.serialize_field("metric", &self.metric)?;
        state.serialize_field("value", &self.sample)?;
        state.end()
    }
}

#[derive(Debug, Clone)]
pub struct RangeValue {
    // XXX-TODO: replace `metric` and `samples` with a single `series: Series` field
    pub metric: Labels,
    pub samples: Vec<Sample>,

    /// Time window, microseconds.
    ///
    /// `time_range.1 - time_range.0` is equal to the time duration that is
    /// appended in square brackets at the end of a range vector selector.
    /// E.g. in the following query
    /// ```promql
    /// http_requests_total{job="prometheus"}[2m]
    /// ```
    /// the duration of the time window is 2 minutes.
    ///
    /// See also
    /// <https://promlabs.com/blog/2020/06/18/the-anatomy-of-a-promql-query/#range-queries>
    pub time_range: Option<(i64, i64)>,
}

impl Serialize for RangeValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RangeValue", 2)?;
        state.serialize_field("metric", &self.metric)?;
        state.serialize_field("values", &self.samples)?;
        state.end()
    }
}

impl RangeValue {
    /// Returns first and last data points, [extrapolated] to the time window
    /// boundaries.
    ///
    /// [extrapolated]: https://promlabs.com/blog/2021/01/29/how-exactly-does-promql-calculate-rates/#extrapolation-of-data
    pub(crate) fn extrapolate(&self) -> Option<(Sample, Sample)> {
        let samples = &self.samples;
        if samples.len() < 2 {
            return None;
        }
        let first = samples.first().unwrap();
        let last = samples.last().unwrap();

        let (t_start, t_end) = self.time_range.unwrap();
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
                    b.sample
                        .value
                        .partial_cmp(&a.sample.value)
                        .unwrap_or(Ordering::Equal)
                });
            }
            Value::Matrix(v) => {
                v.sort_by(|a, b| {
                    let a = a.samples.iter().map(|x| x.value).sum::<f64>();
                    let b = b.samples.iter().map(|x| x.value).sum::<f64>();
                    b.partial_cmp(&a).unwrap_or(Ordering::Equal)
                });
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use expect_test::expect;

    #[test]
    fn test_sample_serialize() {
        let sample = Sample {
            timestamp: 1683138292000000,
            value: 3.14,
        };
        expect![[r#"
            [
              1683138292,
              "3.14"
            ]"#]]
        .assert_eq(&serde_json::to_string_pretty(&sample).unwrap());
    }

    #[test]
    fn test_instant_value_serialize() {
        let ival = InstantValue {
            metric: Labels::new([
                ("__name__", "zo_http_response_time_bucket"),
                ("cloud", "gcp"),
                ("cluster", "zo1"),
                ("endpoint", "/_json"),
                ("exported_instance", "zo1-zincobserve-ingester-0"),
                (
                    "instance",
                    "zo1-zincobserve-ingester.ziox-alpha1.svc.cluster.local:5080",
                ),
                ("job", "zincobserve"),
                ("le", "1"),
                ("metric_type", "histogram"),
                ("namespace", "ziox-alpha1"),
                ("organization", "default"),
                ("role", "ingester"),
                ("status", "200"),
                ("stream", "gke-fluentbit"),
                ("stream_type", "logs"),
            ]),
            sample: Sample {
                timestamp: 1683138292000000,
                value: 3.14,
            },
        };
        expect![[r#"
            {
              "metric": {
                "__name__": "zo_http_response_time_bucket",
                "cloud": "gcp",
                "cluster": "zo1",
                "endpoint": "/_json",
                "exported_instance": "zo1-zincobserve-ingester-0",
                "instance": "zo1-zincobserve-ingester.ziox-alpha1.svc.cluster.local:5080",
                "job": "zincobserve",
                "le": "1",
                "metric_type": "histogram",
                "namespace": "ziox-alpha1",
                "organization": "default",
                "role": "ingester",
                "status": "200",
                "stream": "gke-fluentbit",
                "stream_type": "logs"
              },
              "value": [
                1683138292,
                "3.14"
              ]
            }"#]]
        .assert_eq(&serde_json::to_string_pretty(&ival).unwrap());
    }

    #[test]
    fn test_range_value_serialize() {
        let rval = RangeValue {
            metric: Labels::new([
                ("cloud", "gcp"),
                ("exported_instance", "zo1-zincobserve-ingester-0"),
                ("cluster", "zo1"),
                ("endpoint", "/_json"),
                ("__name__", "zo_http_response_time_bucket"),
            ]),
            samples: vec![
                Sample {
                    timestamp: 1683138292000000,
                    value: 1.1,
                },
                Sample {
                    timestamp: 1683138317000000,
                    value: 2.2,
                },
            ],
            time_range: Some((100, 500)),
        };
        expect![[r#"
            {
              "metric": {
                "__name__": "zo_http_response_time_bucket",
                "cloud": "gcp",
                "cluster": "zo1",
                "endpoint": "/_json",
                "exported_instance": "zo1-zincobserve-ingester-0"
              },
              "values": [
                [
                  1683138292,
                  "1.1"
                ],
                [
                  1683138317,
                  "2.2"
                ]
              ]
            }"#]]
        .assert_eq(&serde_json::to_string_pretty(&rval).unwrap());
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
