use datafusion::error::Result;
use promql_parser::parser::Expr as PromExpr;

use crate::value::Value;
use crate::Engine;

pub async fn bottomk(ctx: &Engine, param: Box<PromExpr>, data: &Value) -> Result<Value> {
    super::eval_top(ctx, param, data, true).await
}
