use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_recursion::async_recursion;
use datafusion::{
    arrow::array::{Float64Array, Int64Array, StringArray},
    error::{DataFusionError, Result},
    prelude::{col, lit, SessionContext},
};
use indexmap::IndexMap;
use promql_parser::{
    label::MatchOp,
    parser::{
        token, AggregateExpr, Call, EvalStmt, Expr as PromExpr, Function, FunctionArgs,
        LabelModifier, MatrixSelector, NumberLiteral, ParenExpr, TokenType, UnaryExpr,
        VectorSelector,
    },
};
use rustc_hash::FxHashMap;

use crate::{
    aggregations,
    datafusion::regexp_udf,
    functions::{self, Func},
    labels::Labels,
    value::*,
};

// See https://docs.rs/indexmap/latest/indexmap/#alternate-hashers
type FxIndexMap<K, V> = IndexMap<K, V, std::hash::BuildHasherDefault<rustc_hash::FxHasher>>;

type MetricsCache = FxHashMap<String, Arc<Vec<Series>>>;

pub struct QueryEngine {
    ctx: Arc<SessionContext>,
    /// The time boundaries for the evaluation. If start equals end an instant
    /// is evaluated.
    start: i64,
    end: i64,
    /// Time between two evaluated instants for the range [start:end].
    interval: i64,
    /// Default look back from sample search.
    lookback_delta: i64,
    /// The index of the current time window. Used when evaluating a [range query].
    ///
    /// [range query]: https://promlabs.com/blog/2020/06/18/the-anatomy-of-a-promql-query/#range-queries
    time_window_idx: i64,
    /// key — metric name; value — time series data
    metrics_cache: MetricsCache,
}

impl QueryEngine {
    pub fn new(ctx: Arc<SessionContext>) -> Self {
        let now = micros_since_epoch(SystemTime::now());
        let five_min = micros(Duration::from_secs(300));
        Self {
            ctx,
            start: now,
            end: now,
            interval: five_min,
            lookback_delta: five_min,
            time_window_idx: 0,
            metrics_cache: Default::default(),
        }
    }

    pub async fn exec(&mut self, stmt: EvalStmt) -> Result<Value> {
        self.start = micros_since_epoch(stmt.start);
        self.end = micros_since_epoch(stmt.end);
        if stmt.interval > Duration::ZERO {
            self.interval = micros(stmt.interval);
        }
        if stmt.lookback_delta > Duration::ZERO {
            self.lookback_delta = micros(stmt.lookback_delta);
        }

        if self.start == self.end {
            // Instant query
            let mut value = self.exec_expr(&stmt.expr).await?;
            if let Value::Float(v) = value {
                return Ok(Value::Sample(Sample {
                    timestamp: self.end,
                    value: v,
                }));
            }
            value.sort();
            return Ok(value);
        }

        // Range query
        // See https://promlabs.com/blog/2020/06/18/the-anatomy-of-a-promql-query/#range-queries
        let mut instant_vectors = Vec::new();
        let nr_steps = ((self.end - self.start) / self.interval) + 1;
        for i in 0..nr_steps {
            self.time_window_idx = i;
            match self.exec_expr(&stmt.expr).await? {
                Value::Instant(v) => instant_vectors.push(RangeValue {
                    metric: v.metric,
                    samples: vec![v.sample],
                    time_range: None,
                }),
                Value::Range(v) => instant_vectors.push(v),
                Value::Vector(vs) => instant_vectors.extend(vs.into_iter().map(|v| RangeValue {
                    metric: v.metric,
                    samples: vec![v.sample],
                    time_range: None,
                })),
                Value::Matrix(vs) => instant_vectors.extend(vs),
                Value::Sample(v) => instant_vectors.push(RangeValue {
                    metric: Labels::default(),
                    samples: vec![v],
                    time_range: None,
                }),
                Value::Float(v) => {
                    let sample = Sample {
                        timestamp: self.start + (self.interval * self.time_window_idx),
                        value: v,
                    };
                    instant_vectors.push(RangeValue {
                        metric: Labels::default(),
                        samples: vec![sample],
                        time_range: None,
                    });
                }
                Value::None => continue,
            };
        }

        // empty result quick return
        if instant_vectors.is_empty() {
            return Ok(Value::None);
        }

        // merge data
        let mut merged_samples = FxHashMap::default();
        let mut merged_metrics = instant_vectors
            .into_iter()
            .map(|rval| {
                let sig = rval.metric.signature();
                merged_samples
                    .entry(sig.clone())
                    .or_insert_with(Vec::new)
                    .extend(rval.samples);
                (sig, rval.metric)
            })
            .collect::<FxHashMap<_, _>>();

        let merged_data = merged_samples
            .into_iter()
            .map(|(sig, samples)| RangeValue {
                metric: merged_metrics.remove(&sig).unwrap(),
                samples,
                time_range: None,
            })
            .collect::<Vec<_>>();
        // We have moved all the values from `merged_metrics` into
        // `merged_data`.
        assert!(merged_metrics.is_empty());

        // sort data
        let mut value = Value::Matrix(merged_data);
        value.sort();
        Ok(value)
    }

    #[async_recursion]
    pub async fn exec_expr(&mut self, prom_expr: &PromExpr) -> Result<Value> {
        Ok(match &prom_expr {
            PromExpr::Aggregate(AggregateExpr {
                op,
                expr,
                param,
                modifier,
            }) => self.aggregate_exprs(op, expr, param, modifier).await?,
            PromExpr::Unary(UnaryExpr { expr }) => {
                let _input = self.exec_expr(expr).await?;
                todo!()
            }
            PromExpr::Binary(_) => todo!(),
            PromExpr::Paren(ParenExpr { expr }) => {
                let _input = self.exec_expr(expr).await?;
                todo!()
            }
            PromExpr::Subquery(_) => todo!(),
            PromExpr::NumberLiteral(NumberLiteral { val }) => Value::Float(*val),
            PromExpr::StringLiteral(_) => todo!(),
            PromExpr::VectorSelector(v) => {
                let data = self.eval_vector_selector(v).await?;
                if data.is_empty() {
                    Value::None
                } else {
                    Value::Vector(data)
                }
            }
            PromExpr::MatrixSelector(MatrixSelector {
                vector_selector,
                range,
            }) => {
                let data = self.eval_matrix_selector(vector_selector, *range).await?;
                if data.is_empty() {
                    Value::None
                } else {
                    Value::Matrix(data)
                }
            }
            PromExpr::Call(Call { func, args }) => self.call_expr(func, args).await?,
            PromExpr::Extension(_) => todo!(),
        })
    }

    async fn eval_vector_selector(
        &mut self,
        selector: &VectorSelector,
    ) -> Result<Vec<InstantValue>> {
        let metrics = self.selector_fetch(selector, None).await?;
        Ok(metrics
            .iter()
            .filter_map(|series| {
                let sample = series.samples.last()?;
                Some(
                    // XXX-FIXME: an instant query can return any valid PromQL
                    // expression type (string, scalar, instant and range vectors).
                    //
                    // See https://promlabs.com/blog/2020/06/18/the-anatomy-of-a-promql-query/#instant-queries
                    InstantValue {
                        metric: series.metric.clone(),
                        sample: *sample,
                    },
                )
            })
            .collect())
    }

    /// MatrixSelector is a special case of VectorSelector that returns a matrix of samples.
    async fn eval_matrix_selector(
        &mut self,
        selector: &VectorSelector,
        range: Duration,
    ) -> Result<Vec<RangeValue>> {
        let metrics = self.selector_fetch(selector, Some(range)).await?;

        let end = self.start + (self.interval * self.time_window_idx); // 15s
        let start = end - micros(range); // 5m

        Ok(metrics
            .iter()
            .map(|series| {
                // XXX-OPTIMIZE: since samples are sorted by timestamp, it may
                // be more efficient to find the [range] of `series.samples`
                // with timestamps within (start; end] and [drain] the rest
                // of the samples.
                //
                // [drain]: https://doc.rust-lang.org/std/vec/struct.Vec.html#method.drain
                // [range]: https://doc.rust-lang.org/std/ops/trait.RangeBounds.html
                let mut samples = series
                    .samples
                    // XXX-OPTIMIZE: avoid clone
                    .clone();
                samples.retain(|v| v.timestamp > start && v.timestamp <= end);
                RangeValue {
                    metric: series.metric.clone(),
                    samples,
                    time_range: Some((start, end)),
                }
            })
            .collect())
    }

    /// Loads time series data from `cache` if it exists, otherwise loads it
    /// from DataFusion and caches.
    async fn selector_fetch(
        &mut self,
        selector: &VectorSelector,
        range: Option<Duration>,
    ) -> Result<Arc<Vec<Series>>> {
        // XXX-TODO: support PromQL queries without metric name, e.g. `{__name__=~"foo.*"}`
        let metric_name = selector.name.as_ref().unwrap();
        if let Some(metrics) = self.metrics_cache.get(metric_name) {
            return Ok(Arc::clone(metrics));
        }
        let metrics = Arc::new(self.selector_load_from_df(selector, range).await?);
        self.metrics_cache
            .insert(metric_name.to_owned(), Arc::clone(&metrics));
        Ok(metrics)
    }

    /// Loads time series data from DataFusion.
    #[tracing::instrument(skip_all)]
    async fn selector_load_from_df(
        &mut self,
        selector: &VectorSelector,
        range: Option<Duration>,
    ) -> Result<Vec<Series>> {
        let table_name = selector.name.as_ref().unwrap();
        let table = self.ctx.table(table_name).await?;

        let start = {
            // https://promlabs.com/blog/2020/07/02/selecting-data-in-promql/#lookback-delta
            let lookback_delta = range.map_or(self.lookback_delta, micros);
            self.start + (self.interval * self.time_window_idx) - lookback_delta
        };
        let end = self.end;

        let mut df = table.clone().filter(
            col(FIELD_TIME)
                .gt(lit(start))
                .and(col(FIELD_TIME).lt_eq(lit(end))),
        )?;
        let regexp_match_udf = regexp_udf::REGEX_MATCH_UDF.clone();
        let regexp_not_match_udf = regexp_udf::REGEX_NOT_MATCH_UDF.clone();
        for mat in selector.matchers.matchers.iter() {
            match &mat.op {
                MatchOp::Equal => {
                    df = df.filter(col(mat.name.clone()).eq(lit(mat.value.clone())))?
                }
                MatchOp::NotEqual => {
                    df = df.filter(col(mat.name.clone()).not_eq(lit(mat.value.clone())))?
                }
                MatchOp::Re(_re) => {
                    df = df.filter(
                        regexp_match_udf.call(vec![col(mat.name.clone()), lit(mat.value.clone())]),
                    )?
                }
                MatchOp::NotRe(_re) => {
                    df = df.filter(
                        regexp_not_match_udf
                            .call(vec![col(mat.name.clone()), lit(mat.value.clone())]),
                    )?
                }
            }
        }
        tracing::info!("loaded metrics 0");

        let batches = df.collect().await?;
        tracing::info!("loaded metrics 1");

        let mut metrics = FxIndexMap::<String, Series>::default();
        for batch in &batches {
            let hash_values = batch
                .column_by_name(FIELD_HASH)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let time_values = batch
                .column_by_name(FIELD_TIME)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let value_values = batch
                .column_by_name(FIELD_VALUE)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                let hash = hash_values.value(i).to_string();
                let entry = metrics.entry(hash).or_insert_with(|| {
                    let metric = Labels::new(
                        batch
                            .schema()
                            .fields()
                            .iter()
                            .zip(batch.columns())
                            .filter_map(|(k, v)| {
                                let name = k.name();
                                if name == FIELD_TIME || name == FIELD_VALUE {
                                    // surprisingly, we can use `return` in a
                                    // closure
                                    return None;
                                }
                                let value = v.as_any().downcast_ref::<StringArray>().unwrap();
                                Some((name.to_string(), value.value(i).to_string()))
                            }),
                    );
                    Series {
                        metric,
                        samples: Vec::with_capacity(20),
                    }
                });
                entry.samples.push(Sample {
                    timestamp: time_values.value(i),
                    value: value_values.value(i),
                });
            }
        }
        tracing::info!("loaded samples");

        let metrics = metrics
            // we don't need the primary key (FIELD_HASH) any more
            .into_values()
            .map(|mut series| {
                if &series.metric[FIELD_TYPE] == TYPE_COUNTER {
                    // Deal with counter resets.
                    // See <https://promlabs.com/blog/2021/01/29/how-exactly-does-promql-calculate-rates/#dealing-with-counter-resets>
                    let mut delta: f64 = 0.0;
                    let mut last_value = 0.0;
                    for sample in series.samples.iter_mut() {
                        if last_value > sample.value {
                            delta += last_value;
                        }
                        last_value = sample.value;
                        if delta > 0.0 {
                            sample.value += delta;
                        }
                    }
                }
                series
            })
            .collect();
        Ok(metrics)
    }

    async fn aggregate_exprs(
        &mut self,
        op: &TokenType,
        expr: &PromExpr,
        param: &Option<Box<PromExpr>>,
        modifier: &Option<LabelModifier>,
    ) -> Result<Value> {
        let sample_time = self.start + (self.interval * self.time_window_idx);
        let input = self.exec_expr(expr).await?;

        Ok(match op.id() {
            token::T_SUM => aggregations::sum(sample_time, modifier, &input)?,
            token::T_AVG => aggregations::avg(sample_time, modifier, &input)?,
            token::T_COUNT => aggregations::count(sample_time, modifier, &input)?,
            token::T_MIN => aggregations::min(sample_time, modifier, &input)?,
            token::T_MAX => aggregations::max(sample_time, modifier, &input)?,
            token::T_GROUP => Value::None,
            token::T_STDDEV => Value::None,
            token::T_STDVAR => Value::None,
            token::T_TOPK => aggregations::topk(self, param.clone().unwrap(), &input).await?,
            token::T_BOTTOMK => aggregations::bottomk(self, param.clone().unwrap(), &input).await?,
            token::T_COUNT_VALUES => Value::None,
            token::T_QUANTILE => Value::None,
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported Aggregate: {:?}",
                    op
                )));
            }
        })
    }

    async fn call_expr(&mut self, func: &Function, args: &FunctionArgs) -> Result<Value> {
        let func_name = Func::from_str(func.name).map_err(|_| {
            DataFusionError::Internal(format!("Unsupported function: {}", func.name))
        })?;

        let last_arg = args
            .last()
            .expect("BUG: promql-parser should have validated function arguments");
        let input = self.exec_expr(&last_arg).await?;

        Ok(match func_name {
            Func::Abs => todo!(),
            Func::Absent => todo!(),
            Func::AbsentOverTime => todo!(),
            Func::AvgOverTime => functions::avg_over_time(&input)?,
            Func::Ceil => todo!(),
            Func::Changes => todo!(),
            Func::Clamp => todo!(),
            Func::ClampMax => todo!(),
            Func::ClampMin => todo!(),
            Func::CountOverTime => functions::count_over_time(&input)?,
            Func::DayOfMonth => todo!(),
            Func::DayOfWeek => todo!(),
            Func::DayOfYear => todo!(),
            Func::DaysInMonth => todo!(),
            Func::Delta => functions::delta(&input)?,
            Func::Deriv => todo!(),
            Func::Exp => todo!(),
            Func::Floor => todo!(),
            Func::HistogramCount => todo!(),
            Func::HistogramFraction => todo!(),
            Func::HistogramQuantile => {
                let args = &args.args;
                if args.len() != 2 {
                    return Err(DataFusionError::Internal(format!(
                        "{}: expected 2 arguments, got {}",
                        func.name,
                        args.len()
                    )));
                }
                let phi = {
                    match *args[0] {
                        PromExpr::NumberLiteral(ref num) => num.val,
                        _ => {
                            return Err(DataFusionError::Internal(format!(
                                "{}: the first argument must be a number",
                                func.name
                            )))
                        }
                    }
                };
                let sample_time = self.start + (self.interval * self.time_window_idx);
                functions::histogram_quantile(sample_time, phi, input)?
            }
            Func::HistogramSum => todo!(),
            Func::HoltWinters => todo!(),
            Func::Hour => todo!(),
            Func::Idelta => functions::idelta(&input)?,
            Func::Increase => functions::increase(&input)?,
            Func::Irate => functions::irate(&input)?,
            Func::LabelJoin => todo!(),
            Func::LabelReplace => todo!(),
            Func::Ln => todo!(),
            Func::Log10 => todo!(),
            Func::Log2 => todo!(),
            Func::MaxOverTime => functions::max_over_time(&input)?,
            Func::MinOverTime => functions::min_over_time(&input)?,
            Func::Minute => todo!(),
            Func::Month => todo!(),
            Func::PredictLinear => todo!(),
            Func::QuantileOverTime => todo!(),
            Func::Rate => functions::rate(&input)?,
            Func::Resets => todo!(),
            Func::Round => todo!(),
            Func::Scalar => todo!(),
            Func::Sgn => todo!(),
            Func::Sort => todo!(),
            Func::SortDesc => todo!(),
            Func::SumOverTime => functions::sum_over_time(&input)?,
            Func::Time => todo!(),
            Func::Timestamp => todo!(),
            Func::Vector => todo!(),
            Func::Year => todo!(),
        })
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
