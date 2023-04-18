use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_recursion::async_recursion;
use datafusion::arrow::json as arrowJson;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{col, lit, SessionContext};
use promql_parser::parser::{
    token, AggModifier, AggregateExpr, BinaryExpr as PromBinaryExpr, Call, EvalStmt,
    Expr as PromExpr, Function, FunctionArgs, MatrixSelector, NumberLiteral, ParenExpr,
    StringLiteral, SubqueryExpr, TokenType, UnaryExpr, VectorSelector,
};
use serde::{Deserialize, Serialize};

pub mod aggregation;
pub mod function;

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
    pub async fn prom_expr_to_plan(&self, prom_expr: PromExpr) -> Result<StackValue> {
        match &prom_expr {
            PromExpr::Aggregate(AggregateExpr {
                op,
                expr,
                param,
                modifier,
            }) => self.aggregate_exprs(op, expr, param, modifier).await,
            PromExpr::Unary(UnaryExpr { expr }) => {
                println!("PromExpr::Unary: {expr:?}");
                let _input = self.prom_expr_to_plan(*expr.clone()).await?;
                Ok(StackValue::None)
            }
            PromExpr::Binary(PromBinaryExpr {
                lhs,
                rhs,
                op,
                modifier,
            }) => {
                println!("PromExpr::Binary: {lhs:?} {rhs:?} {op:?} {modifier:?}");
                Ok(StackValue::None)
            }
            PromExpr::Paren(ParenExpr { expr }) => {
                println!("PromExpr::Paren: {expr:?}");
                let _input = self.prom_expr_to_plan(*expr.clone()).await?;
                Ok(StackValue::None)
            }
            PromExpr::Subquery(SubqueryExpr {
                expr,
                offset,
                at,
                range,
                step,
            }) => {
                println!("PromExpr::Subquery: {expr:?} {offset:?} {at:?} {range:?} {step:?}");
                Ok(StackValue::None)
            }
            PromExpr::NumberLiteral(NumberLiteral { val }) => {
                println!("PromExpr::NumberLiteral: {val:?}");
                Ok(StackValue::NumberLiteral(*val))
            }
            PromExpr::StringLiteral(StringLiteral { .. }) => {
                println!("PromExpr::StringLiteral: NOT SUPPORTED");
                Ok(StackValue::None)
            }
            PromExpr::VectorSelector(VectorSelector {
                name,
                offset,
                matchers,
                at,
            }) => {
                println!("PromExpr::VectorSelector: {name:?} {offset:?} {matchers:?} {at:?}");
                Ok(StackValue::None)
            }
            PromExpr::MatrixSelector(MatrixSelector {
                vector_selector,
                range,
            }) => {
                let data = self.matrix_selector(vector_selector, range).await?;
                Ok(StackValue::MatrixValue(data))
            }
            PromExpr::Call(Call { func, args }) => self.call_expres(func, args).await,
        }
    }

    /// MatrixSelector is a special case of VectorSelector that returns a matrix of samples.
    pub async fn matrix_selector(
        &self,
        selector: &VectorSelector,
        range: &Duration,
    ) -> Result<Vec<VectorValue>> {
        // println!("PromExpr::MatrixSelector: {selector:?} {range:?}");

        // first: calcuate metrics group
        let table_name = selector.name.clone().unwrap();
        let table = self.ctx.table(&table_name).await?;
        let schema = table.schema();
        let fields = schema.fields();
        let mut group_by = Vec::new();
        fields.iter().for_each(|field| {
            if field.name() != "value" && field.name() != "_timestamp" {
                group_by.push(col(field.name()));
            }
        });
        let df_group = table.clone().aggregate(group_by.to_vec(), vec![])?;
        df_group.clone().show().await?;
        let group_data = df_group.collect().await?;
        let json_rows = arrowJson::writer::record_batches_to_json_rows(&group_data[..]).unwrap();
        let mut groups: Vec<HashMap<String, String>> = Vec::new();
        for row in json_rows.iter() {
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
            let mut group_data: Vec<Point> = Vec::new();
            json_rows.iter().for_each(|row| {
                let timestamp = row.get("_timestamp").unwrap().as_i64().unwrap();
                let value = row.get("value").unwrap().as_f64().unwrap();
                group_data.push(Point { timestamp, value });
            });
            let group_data = Rc::new(group_data);

            // fill group
            let start = self.start;
            let end = self.end;
            let interval = self.interval;
            let mut group_points = HashMap::new();
            let mut pos = start;
            while pos < end {
                // println!("-----------------");
                // println!("group: {:?}", group);

                // fill the gap of data of the group
                let start = (pos.duration_since(UNIX_EPOCH).unwrap() - *range).as_micros() as i64;
                let end = (pos.duration_since(UNIX_EPOCH).unwrap()).as_micros() as i64;
                let step_data: Vec<Point> = group_data
                    .clone()
                    .iter()
                    .filter_map(|v| {
                        if v.timestamp > start && v.timestamp <= end {
                            Some(v.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                group_points.insert(end, step_data);
                pos += interval;
            }

            values.push(VectorValue {
                metric: group,
                values: group_points,
            })
        }

        Ok(values)
    }

    pub async fn aggregate_exprs(
        &self,
        op: &TokenType,
        expr: &PromExpr,
        param: &Option<Box<PromExpr>>,
        modifier: &Option<AggModifier>,
    ) -> Result<StackValue> {
        println!("PromExpr::Aggregate: {op:?} {param:?} {modifier:?}");
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

    pub async fn call_expres(&self, func: &Function, args: &FunctionArgs) -> Result<StackValue> {
        // println!("PromExpr::Call: {func:?} {args:?}");
        let mut input: StackValue = StackValue::None;
        for arg in args.args.iter() {
            input = self.prom_expr_to_plan(*arg.clone()).await?;
        }

        Ok(match func.name {
            "increase" => StackValue::None,
            "rate" => function::rate::rate(&input)?,
            "delta" => StackValue::None,
            "idelta" => StackValue::None,
            "irate" => function::irate::irate(&input)?,
            "resets" => StackValue::None,
            "changes" => StackValue::None,
            "avg_over_time" => StackValue::None,
            "min_over_time" => StackValue::None,
            "max_over_time" => StackValue::None,
            "sum_over_time" => StackValue::None,
            "count_over_time" => StackValue::None,
            "last_over_time" => StackValue::None,
            "absent_over_time" => StackValue::None,
            "present_over_time" => StackValue::None,
            "stddev_over_time" => StackValue::None,
            "stdvar_over_time" => StackValue::None,
            "quantile_over_time" => StackValue::None,
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported function: {}",
                    func.name
                )));
            }
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
