use datafusion::error::Result;
use promql_parser::parser::Expr as PromExpr;

use crate::value::Value;
use crate::QueryEngine;

pub async fn topk(ctx: &mut QueryEngine, param: Box<PromExpr>, data: &Value) -> Result<Value> {
    super::eval_top(ctx, param, data, false).await
}