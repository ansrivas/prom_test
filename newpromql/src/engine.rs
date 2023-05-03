use async_recursion::async_recursion;
use datafusion::{
    arrow::json as arrowJson,
    error::{DataFusionError, Result},
    prelude::{col, lit, SessionContext},
};
use promql_parser::{
    label::MatchOp,
    parser::{
        token, AggregateExpr, Call, EvalStmt, Expr as PromExpr, Function, FunctionArgs,
        LabelModifier, MatrixSelector, NumberLiteral, ParenExpr, TokenType, UnaryExpr,
        VectorSelector,
    },
};
use rustc_hash::FxHashMap;
use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{aggregations, functions, value::*};

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
    data_cache: FxHashMap<String, Value>,
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
            data_cache: FxHashMap::default(),
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
                value = Value::Sample(Sample {
                    timestamp: self.end,
                    value: v,
                });
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
                        timestamp: self.start + (self.interval * self.time_window_idx),
                        value: v,
                    }],
                }),
                Value::None => continue,
            };
        }

        // empty result quick return
        if instant_vectors.is_empty() {
            return Ok(Value::None);
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

    /// MatrixSelector is a special case of VectorSelector that returns a matrix of samples.
    async fn eval_vector_selector(
        &mut self,
        selector: &VectorSelector,
    ) -> Result<Vec<InstantValue>> {
        let metrics_name = selector.name.as_ref().unwrap();
        if !self.data_cache.contains_key(metrics_name) {
            self.selector_load_data(selector, None).await?;
        }
        let cache_data = match self.data_cache.get(metrics_name) {
            Some(v) => match v.get_ref_matrix_values() {
                Some(v) => v,
                None => return Ok(vec![]),
            },
            None => return Ok(vec![]),
        };

        let mut values = vec![];
        for metric in cache_data {
            let value = match metric.values.last() {
                Some(v) => *v,
                None => continue, // have no sample
            };
            values.push(
                // XXX-FIXME: an instant query can return any valid PromQL
                // expression type (string, scalar, instant and range vectors).
                //
                // See https://promlabs.com/blog/2020/06/18/the-anatomy-of-a-promql-query/#instant-queries
                InstantValue {
                    labels: metric.labels.clone(),
                    value,
                },
            );
        }
        Ok(values)
    }

    /// MatrixSelector is a special case of VectorSelector that returns a matrix of samples.
    async fn eval_matrix_selector(
        &mut self,
        selector: &VectorSelector,
        range: Duration,
    ) -> Result<Vec<RangeValue>> {
        let metrics_name = selector.name.as_ref().unwrap();
        if !self.data_cache.contains_key(metrics_name) {
            self.selector_load_data(selector, Some(range)).await?;
        }
        let cache_data = match self.data_cache.get(metrics_name) {
            Some(v) => match v.get_ref_matrix_values() {
                Some(v) => v,
                None => return Ok(vec![]),
            },
            None => return Ok(vec![]),
        };

        let end = self.start + (self.interval * self.time_window_idx); // 15s
        let start = end - micros(range); // 5m

        let mut values = Vec::with_capacity(cache_data.len());
        for metric in cache_data {
            let metric_data = metric
                .values
                .iter()
                .filter(|v| v.timestamp > start && v.timestamp <= end)
                .cloned()
                .collect::<Vec<_>>();
            values.push(RangeValue {
                labels: metric.labels.clone(),
                time_range: Some((start, end)),
                values: metric_data,
            });
        }
        Ok(values)
    }

    async fn selector_load_data(
        &mut self,
        selector: &VectorSelector,
        range: Option<Duration>,
    ) -> Result<()> {
        tracing::info!("selector_load_data: start");
        let table_name = selector.name.as_ref().unwrap();
        let metric_table_name = format!("{}_labels", table_name);
        let table = self.ctx.table(metric_table_name).await?;

        let start = {
            // https://promlabs.com/blog/2020/07/02/selecting-data-in-promql/#lookback-delta
            let lookback_delta = range.map_or(self.lookback_delta, micros);
            self.start + (self.interval * self.time_window_idx) - lookback_delta
        };
        let end = self.end; // 30 minutes + 5m = 35m

        // 1. Group by metrics (sets of label name-value pairs)
        let mut df_group = table.clone();
        let regexp_match_udf = super::datafusion::regexp_udf::REGEX_MATCH_UDF.clone();
        let regexp_not_match_udf = super::datafusion::regexp_udf::REGEX_NOT_MATCH_UDF.clone();
        for mat in selector.matchers.matchers.iter() {
            match &mat.op {
                MatchOp::Equal => {
                    df_group = df_group.filter(col(mat.name.clone()).eq(lit(mat.value.clone())))?
                }
                MatchOp::NotEqual => {
                    df_group =
                        df_group.filter(col(mat.name.clone()).not_eq(lit(mat.value.clone())))?
                }
                MatchOp::Re(_re) => {
                    df_group = df_group.filter(
                        regexp_match_udf.call(vec![col(mat.name.clone()), lit(mat.value.clone())]),
                    )?
                }
                MatchOp::NotRe(_re) => {
                    df_group = df_group.filter(
                        regexp_not_match_udf
                            .call(vec![col(mat.name.clone()), lit(mat.value.clone())]),
                    )?
                }
            }
        }
        tracing::info!("selector_load_data: loaded metrics 0");
        let group_data = df_group.collect().await?;
        tracing::info!("selector_load_data: loaded metrics 1");
        let metrics = arrowJson::writer::record_batches_to_json_rows(&group_data)?;
        tracing::info!("selector_load_data: loaded metrics 2");
        // key — the value of FIELD_HASH column; value — time series (`RangeValue`)
        let mut metrics = metrics
            .iter()
            .map(|row| {
                let mut labels = row
                    .iter()
                    .map(|(k, v)| {
                        Arc::new(Label {
                            name: k.to_owned(),
                            value: v.as_str().unwrap().to_owned(),
                        })
                    })
                    .collect::<Vec<_>>();
                labels.sort_by(|a, b| a.name.cmp(&b.name));
                let hash = labels
                    .iter()
                    .find(|x| x.name == FIELD_HASH)
                    .unwrap()
                    .value
                    .clone();
                (
                    hash,
                    RangeValue {
                        labels,
                        time_range: None,
                        // the vector of samples will be filled later
                        values: Vec::with_capacity(20),
                    },
                )
            })
            .collect::<FxHashMap<_, _>>();
        tracing::info!("selector_load_data: loaded metrics 3, {}", metrics.len());

        // 2. Fetch all samples and then group by metrics
        let values_table_name = format!("{}_values", table_name);
        let table = self.ctx.table(values_table_name).await?;
        let mut df_data = table.filter(
            col(FIELD_TIME)
                .gt(lit(start))
                .and(col(FIELD_TIME).lt_eq(lit(end))),
        )?;
        df_data = df_data.select(vec![col(FIELD_HASH), col(FIELD_TIME), col(FIELD_VALUE)])?;
        let metric_data = df_data.collect().await?;
        let metric_data = arrowJson::writer::record_batches_to_json_rows(&metric_data)?;
        tracing::info!("selector_load_data: loaded values");
        for row in metric_data {
            let hash = row.get(FIELD_HASH).unwrap().as_str().unwrap().to_owned();
            // `FIELD_HASH` is a primary key, so we can safely unwrap
            metrics.get_mut(&hash).unwrap().values.push(Sample {
                timestamp: row.get(FIELD_TIME).unwrap().as_i64().unwrap(),
                value: row.get(FIELD_VALUE).unwrap().as_f64().unwrap(),
            });
        }
        // We don't need the primary key (FIELD_HASH) any more
        let mut metric_values = metrics.into_values().collect::<Vec<_>>();
        tracing::info!("selector_load_data: loaded partiton metrics");

        // Fix data about app restart
        for metric in metric_values.iter_mut() {
            let metric_type = metric
                .labels
                .iter()
                .find(|v| v.name == FIELD_TYPE)
                .unwrap()
                .value
                .clone();
            if metric_type != TYPE_COUNTER {
                continue;
            }
            let mut delta: f64 = 0.0;
            let mut last_value = 0.0;
            for sample in metric.values.iter_mut() {
                if last_value > sample.value {
                    delta += last_value;
                }
                last_value = sample.value;
                if delta > 0.0 {
                    sample.value += delta;
                }
            }
        }

        // cache data
        let values = if metric_values.is_empty() {
            Value::None
        } else {
            Value::Matrix(metric_values)
        };
        self.data_cache.insert(table_name.to_string(), values);
        tracing::info!("selector_load_data: loaded cache");
        Ok(())
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
        use crate::functions::Func;

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
