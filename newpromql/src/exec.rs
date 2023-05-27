use datafusion::{error::Result, prelude::SessionContext};
use promql_parser::parser::EvalStmt;
use rustc_hash::FxHashMap;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;

use crate::value::*;

#[derive(Clone)]
pub struct Query {
    pub provider: Arc<SessionContext>,
    /// The time boundaries for the evaluation. If start equals end an instant
    /// is evaluated.
    pub start: i64,
    pub end: i64,
    /// Time between two evaluated instants for the range [start:end].
    pub interval: i64,
    /// Default look back from sample search.
    pub lookback_delta: i64,
    /// key — metric name; value — time series data
    pub data_cache: Arc<RwLock<FxHashMap<String, Value>>>,
}

impl Query {
    pub fn new(provider: Arc<SessionContext>) -> Self {
        let now = micros_since_epoch(SystemTime::now());
        let five_min = micros(Duration::from_secs(300));
        Self {
            provider,
            start: now,
            end: now,
            interval: five_min,
            lookback_delta: five_min,
            data_cache: Arc::new(RwLock::new(FxHashMap::default())),
        }
    }

    pub async fn exec(&mut self, stmt: EvalStmt) -> Result<(Value, Option<String>)> {
        self.start = micros_since_epoch(stmt.start);
        self.end = micros_since_epoch(stmt.end);
        if stmt.interval > Duration::ZERO {
            self.interval = micros(stmt.interval);
        }
        if stmt.lookback_delta > Duration::ZERO {
            self.lookback_delta = micros(stmt.lookback_delta);
        }

        let ctx = Arc::new(self.clone());
        let expr = Arc::new(stmt.expr);
        let mut result_type: Option<String> = None;

        // range query always be matrix result type.
        if self.start != self.end {
            result_type = Some("matrix".to_string());
        } else {
            // Instant query
            let mut engine = super::Engine::new(ctx.clone(), self.start);
            let expr = expr.clone();
            let (mut value, result_type_exec) = engine.exec(&expr).await?;
            if let Value::Float(v) = value {
                value = Value::Sample(Sample {
                    timestamp: self.end,
                    value: v,
                });
            }
            value.sort();
            if result_type_exec.is_some() {
                result_type = result_type_exec;
            }
            return Ok((value, result_type));
        }

        // Range query
        // See https://promlabs.com/blog/2020/06/18/the-anatomy-of-a-promql-query/#range-queries
        let mut instant_vectors = Vec::new();
        let mut tasks = Vec::new();
        let nr_steps = ((self.end - self.start) / self.interval) + 1;
        for i in 0..nr_steps {
            let time = self.start + (self.interval * i);
            let mut engine = super::Engine::new(ctx.clone(), time);
            let expr = expr.clone();
            // let task = tokio::task::spawn(async move { (time, engine.exec_expr(&expr).await) });
            let task = (time, engine.exec(&expr).await);
            tasks.push(task);
        }

        for task in tasks {
            let (time, result) = task;
            let (result, result_type_exec) = result?;
            if result_type.is_none() && result_type_exec.is_some() {
                result_type = result_type_exec;
            }
            match result {
                Value::Instant(v) => instant_vectors.push(RangeValue {
                    labels: v.labels.to_owned(),
                    time_range: None,
                    values: vec![v.value],
                }),
                Value::Vector(vs) => instant_vectors.extend(vs.into_iter().map(|v| RangeValue {
                    labels: v.labels.to_owned(),
                    time_range: None,
                    values: vec![v.value],
                })),
                Value::Range(v) => instant_vectors.push(v),
                Value::Matrix(v) => instant_vectors.extend(v),
                Value::Sample(v) => instant_vectors.push(RangeValue {
                    labels: Labels::default(),
                    time_range: None,
                    values: vec![v],
                }),
                Value::Float(v) => instant_vectors.push(RangeValue {
                    labels: Labels::default(),
                    time_range: None,
                    values: vec![Sample {
                        timestamp: time,
                        value: v,
                    }],
                }),
                Value::None => continue,
            }
        }

        // empty result quick return
        if instant_vectors.is_empty() {
            return Ok((Value::None, result_type));
        }

        // merge data
        let mut merged_data = FxHashMap::default();
        let mut merged_metrics = FxHashMap::default();
        for value in instant_vectors {
            merged_data
                .entry(signature(&value.labels))
                .or_insert_with(Vec::new)
                .extend(value.values);
            merged_metrics.insert(signature(&value.labels), value.labels);
        }
        let merged_data = merged_data
            .into_iter()
            .map(|(sig, values)| RangeValue {
                labels: merged_metrics.get(&sig).unwrap().to_owned(),
                time_range: None,
                values,
            })
            .collect::<Vec<_>>();

        // sort data
        let mut value = Value::Matrix(merged_data);
        value.sort();
        Ok((value, result_type))
    }
}

/// Converts `t` to the number of microseconds elapsed since the beginning of the Unix epoch.
fn micros_since_epoch(t: SystemTime) -> i64 {
    micros(
        t.duration_since(UNIX_EPOCH)
            .expect("BUG: {t} is earlier than Unix epoch"),
    )
}

fn micros(t: Duration) -> i64 {
    t.as_micros()
        .try_into()
        .expect("BUG: time value is too large to fit in i64")
}
