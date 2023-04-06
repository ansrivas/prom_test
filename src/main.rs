use promql::planner::PromPlanner;
use promql_parser::parser::{self, EvalStmt};
use std::{time::{UNIX_EPOCH, Duration}, sync::Arc};
use datafusion::{
    arrow::{
        array::{ArrayRef, Float64Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    datasource::MemTable, prelude::SessionContext,
};

#[tokio::main]
async fn main() {
    let prom_expr =
            parser::parse(&format!("abs(some_metric{{tag_0!=\"bar\"}})")).unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_name = "some_metric";
        let num_tag = 10;
        let num_field = 10;

        let mut columns = vec![];
        columns.push(Field::new("_timestamp", DataType::Int64, true));
        columns.push(Field::new("value", DataType::Float64, true));
        for i in 0..num_tag {
            columns.push(Field::new(format!("tag_{i}"), DataType::Utf8, true));
        }
        for i in 0..num_field {
            columns.push(Field::new(format!("field_{i}"), DataType::Float64, true));
        }
        let schema = Arc::new(Schema::new(columns));

        let mut columns: Vec<ArrayRef> = vec![];
        columns.push(Arc::new(Int64Array::from(vec![1])));
        columns.push(Arc::new(Float64Array::from(vec![1.0])));
        for _i in 0..num_tag {
            columns.push(Arc::new(StringArray::from(vec!["tag"])));
        }
        for _i in 0..num_field {
            columns.push(Arc::new(Float64Array::from(vec![1.0])));
        }
        let batch1 = RecordBatch::try_new(schema.clone(), columns).unwrap();

        // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
        let provider = MemTable::try_new(schema, vec![vec![batch1]]).unwrap();
        let ctx   = SessionContext::new();
         ctx.register_table(table_name, Arc::new(provider)).unwrap();

        let plan = PromPlanner::stmt_to_plan(ctx, eval_stmt)
            .await
            .unwrap();
        println!("{:?}", plan);
}
