use async_recursion::async_recursion;
use datafusion::{
    arrow::array::{Float64Array, Int64Array, StringArray},
    error::{DataFusionError, Result},
    prelude::{col, lit},
};
use promql_parser::{
    label::MatchOp,
    parser::{
        token, AggregateExpr, Call, Expr as PromExpr, Function, FunctionArgs, LabelModifier,
        MatrixSelector, NumberLiteral, ParenExpr, TokenType, UnaryExpr, VectorSelector,
    },
};
use rustc_hash::FxHashMap;
use std::{str::FromStr, sync::Arc, time::Duration};

use crate::{aggregations, functions, value::*};

pub struct Engine {
    ctx: Arc<super::exec::Query>,
    /// The time boundaries for the evaluation.
    time: i64,
    result_type: Option<String>,
}

impl Engine {
    pub fn new(ctx: Arc<super::exec::Query>, time: i64) -> Self {
        Self {
            ctx,
            time,
            result_type: None,
        }
    }

    pub async fn exec(&mut self, prom_expr: &PromExpr) -> Result<(Value, Option<String>)> {
        let value = self.exec_expr(prom_expr).await?;
        Ok((value, self.result_type.clone()))
    }

    #[async_recursion]
    pub async fn exec_expr(&self, prom_expr: &PromExpr) -> Result<Value> {
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
    async fn eval_vector_selector(&self, selector: &VectorSelector) -> Result<Vec<InstantValue>> {
        let metrics_name = selector.name.as_ref().unwrap();
        let need_load_data = { self.ctx.data_cache.read().await.contains_key(metrics_name) };
        if !need_load_data {
            self.selector_load_data(selector, None).await?;
        }
        let metrics_cache = self.ctx.data_cache.read().await;
        let metrics_cache = match metrics_cache.get(metrics_name) {
            Some(v) => match v.get_ref_matrix_values() {
                Some(v) => v,
                None => return Ok(vec![]),
            },
            None => return Ok(vec![]),
        };

        let mut values = vec![];
        for metric in metrics_cache {
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
        &self,
        selector: &VectorSelector,
        range: Duration,
    ) -> Result<Vec<RangeValue>> {
        let metrics_name = selector.name.as_ref().unwrap();
        let need_load_data = { self.ctx.data_cache.read().await.contains_key(metrics_name) };
        if !need_load_data {
            self.selector_load_data(selector, None).await?;
        }
        let metrics_cache = self.ctx.data_cache.read().await;
        let metrics_cache = match metrics_cache.get(metrics_name) {
            Some(v) => match v.get_ref_matrix_values() {
                Some(v) => v,
                None => return Ok(vec![]),
            },
            None => return Ok(vec![]),
        };

        let end = self.time;
        let start = end - micros(range); // -5m

        let mut values = Vec::with_capacity(metrics_cache.len());
        for metric in metrics_cache {
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
        &self,
        selector: &VectorSelector,
        range: Option<Duration>,
    ) -> Result<()> {
        // https://promlabs.com/blog/2020/07/02/selecting-data-in-promql/#lookback-delta
        let start = self.ctx.start - range.map_or(self.ctx.lookback_delta, micros);
        let end = self.ctx.end; // 30 minutes + 5m = 35m

        // 1. Group by metrics (sets of label name-value pairs)
        let table_name = selector.name.as_ref().unwrap();
        let table = self.ctx.provider.table(table_name).await?;
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
        let batches = df_group.collect().await?;

        let mut metrics: FxHashMap<String, RangeValue> = FxHashMap::default();
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
                    let mut labels = Vec::with_capacity(batch.num_columns());
                    for (k, v) in batch.schema().fields().iter().zip(batch.columns()) {
                        let name = k.name();
                        if name == FIELD_HASH || name == FIELD_TIME || name == FIELD_VALUE {
                            continue;
                        }
                        let value = v.as_any().downcast_ref::<StringArray>().unwrap();
                        labels.push(Arc::new(Label {
                            name: name.to_string(),
                            value: value.value(i).to_string(),
                        }));
                    }
                    labels.sort_by(|a, b| a.name.cmp(&b.name));
                    RangeValue {
                        labels,
                        time_range: None,
                        values: Vec::with_capacity(20),
                    }
                });
                entry.values.push(Sample {
                    timestamp: time_values.value(i),
                    value: value_values.value(i),
                });
            }
        }

        // We don't need the primary key (FIELD_HASH) any more
        let mut metric_values = metrics.into_values().collect::<Vec<_>>();

        // Fix data about app restart
        for series in metric_values.iter_mut() {
            if let Some(metric_type) = labels_value(&series.labels, FIELD_TYPE) {
                if metric_type != TYPE_COUNTER {
                    continue;
                }
            }
            let mut delta: f64 = 0.0;
            let mut last_value = 0.0;
            for sample in series.values.iter_mut() {
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
        self.ctx
            .data_cache
            .write()
            .await
            .insert(table_name.to_string(), values);
        Ok(())
    }

    async fn aggregate_exprs(
        &self,
        op: &TokenType,
        expr: &PromExpr,
        param: &Option<Box<PromExpr>>,
        modifier: &Option<LabelModifier>,
    ) -> Result<Value> {
        let sample_time = self.time;
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

    async fn call_expr(&self, func: &Function, args: &FunctionArgs) -> Result<Value> {
        use crate::functions::Func;

        let func_name = Func::from_str(func.name).map_err(|_| {
            DataFusionError::Internal(format!("Unsupported function: {}", func.name))
        })?;

        let last_arg = args
            .last()
            .expect("BUG: promql-parser should have validated function arguments");
        let input = self.exec_expr(&last_arg).await?;

        let ret = match func_name {
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
                let sample_time = self.time;
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
        };
        Ok(
            // All range vector functions drop the metric name in the output.
            //
            // Reference: https://github.com/prometheus/prometheus/blob/3c4802635d05af47ddf7358c0c079961b011c47b/promql/engine.go#L1458-L1464
            ret.drop_metric_name(),
        )
    }
}

fn micros(t: Duration) -> i64 {
    t.as_micros()
        .try_into()
        .expect("BUG: time value is too large to fit in i64")
}
