use async_recursion::async_recursion;
use datafusion::{
    arrow::json as arrowJson,
    error::{DataFusionError, Result},
    prelude::{col, lit, min, SessionContext},
};
use promql_parser::{
    label::MatchOp,
    parser::{
        token, AggModifier, AggregateExpr, Call, EvalStmt, Expr as PromExpr, Function,
        FunctionArgs, MatrixSelector, NumberLiteral, ParenExpr, TokenType, UnaryExpr,
        VectorSelector,
    },
};
use std::{
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{aggregations, functions, value::*};

pub struct QueryEngine {
    ctx: SessionContext,
    /// The time boundaries for the evaluation. If start equals end an instant
    /// is evaluated.
    start: i64,
    end: i64,
    /// Time between two evaluated instants for the range [start:end].
    interval: i64,
    /// Default look back from sample search.
    lookback_delta: i64,
    exec_i: i64,
    data_cache: Arc<HashMap<String, Value>>,
}

impl QueryEngine {
    pub fn new(mut ctx: SessionContext) -> Self {
        // register regexp match
        super::datafusion::register_udf(&mut ctx);
        let ctx = ctx;

        let now = micros_since_epoch(SystemTime::now());
        let five_min = micros(Duration::from_secs(300));
        Self {
            ctx,
            start: now,
            end: now,
            interval: five_min,
            lookback_delta: five_min,
            exec_i: 0,
            data_cache: Arc::new(HashMap::new()),
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
            // instant query
            return self.exec_expr(&stmt.expr).await;
        }

        // range query
        let mut instant_vectors = Vec::new();
        for i in 0..((self.end - self.start) / self.interval) + 1 {
            self.exec_i = i;
            if let Value::VectorValues(in_vec) = self.exec_expr(&stmt.expr).await? {
                instant_vectors.push(in_vec);
            }
        }

        // merge data
        let mut merged_data = HashMap::new();
        let mut merged_metrics = HashMap::new();
        for ivec in instant_vectors {
            for value in ivec {
                let entry = merged_data
                    .entry(signature(&value.metric))
                    .or_insert_with(Vec::new);
                entry.push(value.value);
                merged_metrics.insert(signature(&value.metric), value.metric);
            }
        }
        let merged_data = merged_data
            .into_iter()
            .map(|(metric, values)| RangeValue {
                metric: merged_metrics.get(&metric).unwrap().to_owned(),
                time: None,
                values,
            })
            .collect::<Vec<_>>();

        Ok(Value::MatrixValues(merged_data))
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
            PromExpr::NumberLiteral(NumberLiteral { val }) => Value::NumberLiteral(*val),
            PromExpr::StringLiteral(_) => todo!(),
            PromExpr::VectorSelector(v) => {
                let data = self.eval_vector_selector(v).await?;
                if data.is_empty() {
                    Value::None
                } else {
                    Value::VectorValues(data)
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
                    Value::MatrixValues(data)
                }
            }
            PromExpr::Call(Call { func, args }) => self.call_expr(func, args).await?,
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
                    metric: metric.metric.clone(),
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

        let end = self.start + (self.interval * self.exec_i); // 15s
        let start = end - micros(range); // 5m

        let mut values = vec![];
        for metric in cache_data {
            let metric_data = metric
                .values
                .iter()
                .filter(|v| v.timestamp > start && v.timestamp <= end)
                .cloned()
                .collect::<Vec<_>>();
            values.push(RangeValue {
                metric: metric.metric.clone(),
                time: Some((start, end)),
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
        let table_name = selector.name.as_ref().unwrap();
        let table = self.ctx.table(table_name).await?;

        let start = match range {
            Some(range) => self.start + (self.interval * self.exec_i) - micros(range),
            None => self.start + (self.interval * self.exec_i) - self.lookback_delta,
        };
        let end = self.end; // 30 minutes + 5m = 35m

        // 1. Group by metrics (sets of label name-value pairs)
        let mut df_group = table.clone().filter(
            col(FIELD_TIME)
                .gt(lit(start))
                .and(col(FIELD_TIME).lt_eq(lit(end))),
        )?;
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
        let group_select = table
            .schema()
            .fields()
            .iter()
            .map(datafusion::common::DFField::name)
            .filter(|&field_name| {
                field_name != FIELD_HASH && field_name != FIELD_TIME && field_name != FIELD_VALUE
            })
            .map(|v| min(col(v)).alias(v))
            .collect::<Vec<_>>();
        df_group = df_group.aggregate(vec![col(FIELD_HASH)], group_select)?;
        let group_data = df_group.collect().await?;
        let metrics = arrowJson::writer::record_batches_to_json_rows(&group_data)?
            .iter()
            .map(|row| {
                row.iter()
                    .map(|(k, v)| (k.to_owned(), v.as_str().unwrap().to_owned()))
                    .collect::<HashMap<_, _>>()
            })
            .collect::<Vec<_>>();

        // 2. Fetch all samples and then group by metrics
        let mut df_data = table.clone().filter(
            col(FIELD_TIME)
                .gt(lit(start))
                .and(col(FIELD_TIME).lt_eq(lit(end))),
        )?;
        for mat in selector.matchers.matchers.iter() {
            match &mat.op {
                MatchOp::Equal => {
                    df_data = df_data.filter(col(mat.name.clone()).eq(lit(mat.value.clone())))?
                }
                MatchOp::NotEqual => {
                    df_data =
                        df_data.filter(col(mat.name.clone()).not_eq(lit(mat.value.clone())))?
                }
                MatchOp::Re(_re) => {
                    df_data = df_data.filter(
                        regexp_match_udf.call(vec![col(mat.name.clone()), lit(mat.value.clone())]),
                    )?
                }
                MatchOp::NotRe(_re) => {
                    df_data = df_data.filter(
                        regexp_not_match_udf
                            .call(vec![col(mat.name.clone()), lit(mat.value.clone())]),
                    )?
                }
            }
        }
        df_data = df_data.select(vec![col(FIELD_HASH), col(FIELD_TIME), col(FIELD_VALUE)])?;
        let metric_data = df_data.collect().await?;
        let metric_data = arrowJson::writer::record_batches_to_json_rows(&metric_data)?;
        let mut metric_data_group: HashMap<String, Vec<Sample>> =
            HashMap::with_capacity(metrics.len());
        metric_data.iter().for_each(|row| {
            let hash_value = row.get(FIELD_HASH).unwrap().as_str().unwrap().to_owned();
            let entry = metric_data_group.entry(hash_value).or_default();
            entry.push(Sample {
                timestamp: row.get(FIELD_TIME).unwrap().as_i64().unwrap(),
                value: row.get(FIELD_VALUE).unwrap().as_f64().unwrap(),
            });
        });

        let mut metric_values = Vec::with_capacity(metrics.len());
        for metric in metrics {
            let hash_value = metric.get(FIELD_HASH).unwrap().as_str();
            let metric_data = metric_data_group.get(hash_value).unwrap();
            metric_values.push(RangeValue {
                metric,
                time: None,
                values: metric_data.clone(),
            });
        }

        // Fix data about app restart
        for metric in metric_values.iter_mut() {
            if metric.metric.get(FIELD_TYPE).unwrap().as_str() != TYPE_COUNTER {
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
            Value::MatrixValues(metric_values)
        };
        let cache = Arc::get_mut(&mut self.data_cache).unwrap();
        cache.insert(table_name.to_string(), values);
        Ok(())
    }

    async fn aggregate_exprs(
        &mut self,
        op: &TokenType,
        expr: &PromExpr,
        param: &Option<Box<PromExpr>>,
        modifier: &Option<AggModifier>,
    ) -> Result<Value> {
        let sample_time = self.start + (self.interval * self.exec_i);
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
                functions::histogram_quantile(phi, input)?
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
