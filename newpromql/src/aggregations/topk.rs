use datafusion::error::Result;
use promql_parser::parser::Expr as PromExpr;

use crate::value::Value;
use crate::Engine;

pub async fn topk(ctx: &Engine, param: Box<PromExpr>, data: &Value) -> Result<Value> {
    super::eval_top(ctx, param, data, false).await
}
