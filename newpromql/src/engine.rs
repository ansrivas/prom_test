use async_recursion::async_recursion;
use datafusion::{
    arrow::json as arrowJson,
    error::{DataFusionError, Result},
    prelude::{col, lit, SessionContext},
};
use promql_parser::parser::{
    token, AggModifier, AggregateExpr, Call, EvalStmt, Expr as PromExpr, Function, FunctionArgs,
    MatrixSelector, NumberLiteral, ParenExpr, TokenType, UnaryExpr, VectorSelector,
};
use std::{
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{functions, value::*};

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
    exec_max: i64,
    data_cache: Arc<Value>,
}

impl QueryEngine {
    pub fn new(ctx: SessionContext) -> Self {
        Self {
            ctx,
            start: (SystemTime::now().duration_since(UNIX_EPOCH).unwrap()).as_micros() as i64,
            end: (SystemTime::now().duration_since(UNIX_EPOCH).unwrap()).as_micros() as i64,
            interval: Duration::from_secs(300).as_micros() as i64,
            lookback_delta: Duration::from_secs(300).as_micros() as i64,
            exec_i: 0,
            exec_max: 1,
            data_cache: Arc::new(Value::None),
        }
    }

    pub async fn exec(&mut self, stmt: EvalStmt) -> Result<Value> {
        let start_time = time::Instant::now();
        self.start = (stmt.start.duration_since(UNIX_EPOCH).unwrap()).as_micros() as i64;
        self.end = (stmt.end.duration_since(UNIX_EPOCH).unwrap()).as_micros() as i64;
        if stmt.interval > Duration::from_secs(0) {
            self.interval = stmt.interval.as_micros() as i64;
        }
        if stmt.lookback_delta > Duration::from_secs(0) {
            self.lookback_delta = stmt.lookback_delta.as_micros() as i64;
        }

        // instant query
        let instant_query = self.start == self.end;
        if instant_query {
            let data = self.exec_expr(stmt.expr).await?;
            return Ok(data);
        }

        // range query
        let mut datas = Vec::new();
        self.exec_i = 0;
        self.exec_max = ((self.end - self.start) / self.interval) + 1;
        while self.exec_i < self.exec_max {
            if let Value::VectorValues(data) = self.exec_expr(stmt.expr.clone()).await? {
                datas.push(data);
            }
            self.exec_i += 1;
            tracing::info!(
                "execute exec_i {}, time: {}",
                self.exec_i,
                start_time.elapsed()
            );
        }
        // merge data
        let mut merged_data = HashMap::new();
        let mut merged_metrics = HashMap::new();
        for data in datas {
            for value in data {
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
                values,
            })
            .collect::<Vec<_>>();

        Ok(Value::MatrixValues(merged_data))
    }

    #[async_recursion]
    async fn exec_expr(&mut self, prom_expr: PromExpr) -> Result<Value> {
        Ok(match &prom_expr {
            PromExpr::Aggregate(AggregateExpr {
                op,
                expr,
                param,
                modifier,
            }) => self.aggregate_exprs(op, expr, param, modifier).await?,
            PromExpr::Unary(UnaryExpr { expr }) => {
                let _input = self.exec_expr(*expr.clone()).await?;
                todo!()
            }
            PromExpr::Binary(_) => todo!(),
            PromExpr::Paren(ParenExpr { expr }) => {
                let _input = self.exec_expr(*expr.clone()).await?;
                todo!()
            }
            PromExpr::Subquery(_) => todo!(),
            PromExpr::NumberLiteral(NumberLiteral { val }) => Value::NumberLiteral(*val),
            PromExpr::StringLiteral(_) => todo!(),
            PromExpr::VectorSelector(v) => {
                let data = self.vector_selector(v).await?;
                Value::VectorValues(data)
            }
            PromExpr::MatrixSelector(MatrixSelector {
                vector_selector,
                range,
            }) => {
                let data = self.matrix_selector(vector_selector, *range).await?;
                Value::MatrixValues(data)
            }
            PromExpr::Call(Call { func, args }) => self.call_expr(func, args).await?,
        })
    }

    /// MatrixSelector is a special case of VectorSelector that returns a matrix of samples.
    async fn vector_selector(&mut self, selector: &VectorSelector) -> Result<Vec<InstantValue>> {
        if self.data_cache.is_empty() {
            self.selector_load_data(selector, None).await?;
        }
        let cache_data = self.data_cache.get_ref_matrix_values().unwrap();

        let mut values = vec![];
        for metric in cache_data {
            let value = match metric.values.last() {
                Some(v) => v.clone(),
                None => Sample {
                    timestamp: self.end,
                    value: 0.0,
                },
            };
            values.push(InstantValue {
                metric: metric.metric.clone(),
                value,
            });
        }
        Ok(values)
    }

    /// MatrixSelector is a special case of VectorSelector that returns a matrix of samples.
    async fn matrix_selector(
        &mut self,
        selector: &VectorSelector,
        range: Duration,
    ) -> Result<Vec<RangeValue>> {
        if self.data_cache.is_empty() {
            self.selector_load_data(selector, Some(range)).await?;
        }
        let cache_data = self.data_cache.get_ref_matrix_values().unwrap();

        let start = self.start + (self.interval * self.exec_i) - range.as_micros() as i64;
        let end = self.start + (self.interval * self.exec_i) + self.interval;

        let mut values = vec![];
        for metric in cache_data {
            let metric_data = metric
                .values
                .iter()
                .filter(|v| v.timestamp > start && v.timestamp <= end)
                .map(|v| v.clone())
                .collect::<Vec<_>>();
            values.push(RangeValue {
                metric: metric.metric.clone(),
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
            Some(range) => self.start + (self.interval * self.exec_i) - range.as_micros() as i64,
            None => self.start + (self.interval * self.exec_i) - self.lookback_delta,
        };
        let end = self.end;

        let mut df_group = table.clone().filter(
            col(FIELD_TIME)
                .gt(lit(start))
                .and(col(FIELD_TIME).lt_eq(lit(end))),
        )?;
        for mat in selector.matchers.matchers.iter() {
            df_group = df_group.filter(col(mat.name.clone()).eq(lit(mat.value.clone())))?;
        }
        // 1. Group by metrics (sets of label name-value pairs)
        let group_by = table
            .schema()
            .fields()
            .iter()
            .map(datafusion::common::DFField::name)
            .filter(|&field_name| field_name != FIELD_TIME && field_name != FIELD_VALUE)
            .map(col)
            .collect::<Vec<_>>();
        // data set, the fewer comparison operations the aggregator has to make.
        df_group = df_group.aggregate(group_by, vec![])?;
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
            df_data = df_data.filter(col(mat.name.clone()).eq(lit(mat.value.clone())))?;
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
                values: metric_data.clone(),
            });
        }

        // Fix data about app restart
        for metric in metric_values.iter_mut() {
            if metric.metric.get(FIELD_TYPE).unwrap().as_str() != "counter" {
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
        self.data_cache = Arc::new(Value::MatrixValues(metric_values));
        Ok(())
    }

    async fn aggregate_exprs(
        &mut self,
        op: &TokenType,
        expr: &PromExpr,
        param: &Option<Box<PromExpr>>,
        _modifier: &Option<AggModifier>,
    ) -> Result<Value> {
        let param = param.clone().unwrap().clone();
        let param = self.exec_expr(*param.clone()).await?;
        let param = match param {
            Value::NumberLiteral(v) => v,
            _ => {
                return Err(DataFusionError::Internal(
                    "aggregate param must be NumberLiteral".to_string(),
                ))
            }
        };
        let input = self.exec_expr(expr.clone()).await?;

        Ok(match op.id() {
            token::T_SUM => Value::None,
            token::T_AVG => Value::None,
            token::T_COUNT => Value::None,
            token::T_MIN => Value::None,
            token::T_MAX => Value::None,
            token::T_GROUP => Value::None,
            token::T_STDDEV => Value::None,
            token::T_STDVAR => Value::None,
            token::T_TOPK => crate::aggregation::topk(param as usize, &input)?,
            token::T_BOTTOMK => Value::None,
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
        let input = self.exec_expr(*last_arg).await?;

        Ok(match func_name {
            Func::Abs => todo!(),
            Func::Absent => todo!(),
            Func::AbsentOverTime => todo!(),
            Func::Ceil => todo!(),
            Func::Changes => todo!(),
            Func::Clamp => todo!(),
            Func::ClampMax => todo!(),
            Func::ClampMin => todo!(),
            Func::CountOverTime => todo!(),
            Func::DayOfMonth => todo!(),
            Func::DayOfWeek => todo!(),
            Func::DayOfYear => todo!(),
            Func::DaysInMonth => todo!(),
            Func::Delta => todo!(),
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
            Func::Idelta => todo!(),
            Func::Increase => todo!(),
            Func::Irate => functions::irate(self.start + (self.interval * self.exec_i), &input)?,
            Func::LabelJoin => todo!(),
            Func::LabelReplace => todo!(),
            Func::Ln => todo!(),
            Func::Log2 => todo!(),
            Func::Log10 => todo!(),
            Func::Minute => todo!(),
            Func::Month => todo!(),
            Func::PredictLinear => todo!(),
            Func::QuantileOverTime => todo!(),
            Func::Rate => functions::rate(self.start + (self.interval * self.exec_i), &input)?,
            Func::Resets => todo!(),
            Func::Round => todo!(),
            Func::Scalar => todo!(),
            Func::Sgn => todo!(),
            Func::Sort => todo!(),
            Func::SortDesc => todo!(),
            Func::Time => todo!(),
            Func::Timestamp => todo!(),
            Func::Vector => todo!(),
            Func::Year => todo!(),
        })
    }
}
