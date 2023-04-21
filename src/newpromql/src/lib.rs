use std::collections::HashMap;
use std::rc::Rc;
use std::str::FromStr as _;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_recursion::async_recursion;
use datafusion::arrow::json as arrowJson;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{col, lit, SessionContext};
use promql_parser::parser::{
    token, AggModifier, AggregateExpr, Call, EvalStmt, Expr as PromExpr, Function, FunctionArgs,
    MatrixSelector, NumberLiteral, ParenExpr, TokenType, UnaryExpr, VectorSelector,
};
use serde::{Deserialize, Serialize};

mod aggregation;
mod functions;

pub struct QueryEngine {
    ctx: SessionContext,
    /// The time boundaries for the evaluation. If start equals end an instant
    /// is evaluated.
    pub start: SystemTime,
    pub end: SystemTime,
    /// Time between two evaluated instants for the range [start:end].
    pub interval: Duration,
}

impl QueryEngine {
    pub fn new(ctx: SessionContext) -> Self {
        QueryEngine {
            ctx,
            start: SystemTime::now(),
            end: SystemTime::now(),
            interval: Duration::from_secs(300),
        }
    }

    pub async fn exec(&mut self, stmt: EvalStmt) -> Result<StackValue> {
        self.start = stmt.start;
        self.end = stmt.end;
        self.interval = stmt.interval; // step
        self.prom_expr_to_plan(stmt.expr).await
    }

    #[async_recursion]
    async fn prom_expr_to_plan(&self, prom_expr: PromExpr) -> Result<StackValue> {
        Ok(match &prom_expr {
            PromExpr::Aggregate(AggregateExpr {
                op,
                expr,
                param,
                modifier,
            }) => self.aggregate_exprs(op, expr, param, modifier).await?,
            PromExpr::Unary(UnaryExpr { expr }) => {
                let _input = self.prom_expr_to_plan(*expr.clone()).await?;
                StackValue::None
            }
            PromExpr::Binary(_) => StackValue::None,
            PromExpr::Paren(ParenExpr { expr }) => {
                let _input = self.prom_expr_to_plan(*expr.clone()).await?;
                StackValue::None
            }
            PromExpr::Subquery(_) => StackValue::None,
            PromExpr::NumberLiteral(NumberLiteral { val }) => StackValue::NumberLiteral(*val),
            PromExpr::StringLiteral(_) => StackValue::None,
            PromExpr::VectorSelector(_) => StackValue::None,
            PromExpr::MatrixSelector(MatrixSelector {
                vector_selector,
                range,
            }) => {
                let data = self.matrix_selector(vector_selector, range).await?;
                StackValue::MatrixValue(data)
            }
            PromExpr::Call(Call { func, args }) => self.call_expr(func, args).await?,
        })
    }

    /// MatrixSelector is a special case of VectorSelector that returns a matrix of samples.
    async fn matrix_selector(
        &self,
        selector: &VectorSelector,
        range: &Duration,
    ) -> Result<Vec<VectorValue>> {
        // first: calculate metrics group
        let table_name = selector.name.as_ref().unwrap();
        let table = self.ctx.table(table_name).await?;
        let group_by = table
            .schema()
            .fields()
            .iter()
            .filter_map(|field| {
                let field_name = field.name();
                (field_name != "value" && field_name != "_timestamp").then(|| col(field_name))
            })
            .collect::<Vec<_>>();

        let df_group = table.clone().aggregate(group_by, vec![])?;
        //XXX df_group.clone().show().await?;
        let group_data = df_group.collect().await?;
        //XXX datafusion::arrow::util::pretty::print_batches(&group_data[..2]).unwrap(); // XXX-DELETEME

        let json_rows = arrowJson::writer::record_batches_to_json_rows(&group_data[..]).unwrap();
        let mut groups: Vec<HashMap<String, String>> = Vec::new();
        for row in json_rows {
            let mut group = HashMap::new();
            for (key, value) in row.iter() {
                group.insert(key.to_string(), value.as_str().unwrap().to_string());
            }
            groups.push(group);
        }

        // second: fill each group data
        let mut values = Vec::new();
        for group in groups {
            // fetch all data for the group
            let mut df_data = table.clone().filter(
                col("_timestamp")
                    .gt_eq(lit(
                        (self.start.duration_since(UNIX_EPOCH).unwrap()).as_micros() as i64,
                    ))
                    .and(col("_timestamp").lt(lit(
                        (self.end.duration_since(UNIX_EPOCH).unwrap()).as_micros() as i64,
                    ))),
            )?;
            for mat in selector.matchers.matchers.iter() {
                df_data = df_data.filter(col(mat.name.clone()).eq(lit(mat.value.clone())))?;
            }
            for (key, value) in group.iter() {
                df_data = df_data.filter(col(key.clone()).eq(lit(value.clone())))?;
            }
            df_data = df_data.select(vec![col("_timestamp"), col("value")])?;
            let df_data = df_data.collect().await?;

            let json_rows = arrowJson::writer::record_batches_to_json_rows(&df_data[..]).unwrap();
            let group_data = Rc::new(
                json_rows
                    .into_iter()
                    .map(|row| Point {
                        timestamp: row.get("_timestamp").unwrap().as_i64().unwrap(),
                        value: row.get("value").unwrap().as_f64().unwrap(),
                    })
                    .collect::<Vec<_>>(),
            );

            // fill group
            let mut group_points = HashMap::new();
            let mut pos = self.start;
            while pos < self.end {
                // fill the gap of data of the group
                let start = (pos.duration_since(UNIX_EPOCH).unwrap() - *range).as_micros() as i64;
                let end = (pos.duration_since(UNIX_EPOCH).unwrap()).as_micros() as i64;
                let step_data = group_data
                    .clone()
                    .iter()
                    .filter(|v| v.timestamp > start && v.timestamp <= end)
                    .cloned()
                    .collect();
                group_points.insert(end, step_data);
                pos += self.interval;
            }

            values.push(VectorValue {
                metric: group,
                values: group_points,
            })
        }
        Ok(values)
    }

    async fn aggregate_exprs(
        &self,
        op: &TokenType,
        expr: &PromExpr,
        param: &Option<Box<PromExpr>>,
        _modifier: &Option<AggModifier>,
    ) -> Result<StackValue> {
        let param = param.clone().unwrap().clone();
        let param = self.prom_expr_to_plan(*param.clone()).await?;
        let param = match param {
            StackValue::NumberLiteral(v) => v,
            _ => {
                return Err(DataFusionError::Internal(
                    "aggregate param must be NumberLiteral".to_string(),
                ))
            }
        };
        let input = self.prom_expr_to_plan(expr.clone()).await?;

        Ok(match op.id() {
            token::T_SUM => StackValue::None,
            token::T_AVG => StackValue::None,
            token::T_COUNT => StackValue::None,
            token::T_MIN => StackValue::None,
            token::T_MAX => StackValue::None,
            token::T_GROUP => StackValue::None,
            token::T_STDDEV => StackValue::None,
            token::T_STDVAR => StackValue::None,
            token::T_TOPK => aggregation::topk(param as usize, &input)?,
            token::T_BOTTOMK => StackValue::None,
            token::T_COUNT_VALUES => StackValue::None,
            token::T_QUANTILE => StackValue::None,
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported Aggregate: {:?}",
                    op
                )));
            }
        })
    }

    async fn call_expr(&self, func: &Function, args: &FunctionArgs) -> Result<StackValue> {
        use functions::Func;

        let func_name = Func::from_str(func.name).map_err(|_| {
            DataFusionError::Internal(format!("Unsupported function: {}", func.name))
        })?;

        let last_arg = args
            .last()
            .expect("BUG: promql-parser should have validated function arguments");
        let input = self.prom_expr_to_plan(*last_arg).await?;

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
            Func::HistogramQuantile => todo!(),
            Func::HistogramSum => todo!(),
            Func::HoltWinters => todo!(),
            Func::Hour => todo!(),
            Func::Idelta => todo!(),
            Func::Increase => todo!(),
            Func::Irate => functions::irate(&input)?,
            Func::LabelJoin => todo!(),
            Func::LabelReplace => todo!(),
            Func::Ln => todo!(),
            Func::Log2 => todo!(),
            Func::Log10 => todo!(),
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
            Func::Time => todo!(),
            Func::Timestamp => todo!(),
            Func::Vector => todo!(),
            Func::Year => todo!(),
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VectorValue {
    pub metric: HashMap<String, String>,
    pub values: HashMap<i64, Vec<Point>>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VectorValueResponse {
    pub metric: HashMap<String, String>,
    pub values: Vec<Point>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Point {
    pub timestamp: i64,
    pub value: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StackValue {
    VectorValue(VectorValue),
    MatrixValue(Vec<VectorValue>),
    MatrixValueResponse(Vec<VectorValueResponse>),
    NumberLiteral(f64),
    None,
}
