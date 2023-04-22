use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use arrow_array::{ArrayRef, Float64Array};
use clap::Parser;
use datafusion::{
    arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    datasource::MemTable,
    error::{DataFusionError, Result},
    prelude::SessionContext,
};
use promql_parser::parser;
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
struct Cli {
    #[arg(help = r#"PromQL expression

Examples:
    irate(zo_response_time_count{cluster="zo1"}[5m])
    topk(1, irate(zo_response_time_count{cluster="zo1"}[5m]))
    histogram_quantile(0.9, rate(zo_response_time_bucket[5m]))"#)]
    expr: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let start_time = time::Instant::now();

    let prom_expr = parser::parse(&cli.expr).unwrap();
    //XXX dbg!(&prom_expr);
    std::fs::write("/tmp/XXX.ast.rs", format!("{prom_expr:#?}\n")).unwrap(); // XXX-DELETEME

    let eval_stmt = parser::EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH
            .checked_add(Duration::from_secs(1681711100))
            .unwrap(),
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(1681713200))
            .unwrap(),
        interval: Duration::from_secs(15), // step
        lookback_delta: Duration::from_secs(300),
    };

    let ctx = create_context().unwrap();
    tracing::info!("prepare time: {}", start_time.elapsed());

    let mut engine = newpromql::QueryEngine::new(ctx);
    let data = engine.exec(eval_stmt).await.unwrap();
    dbg!(data);
    tracing::info!("execute time: {}", start_time.elapsed());
}

// create local session context with an in-memory table
fn create_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    let paths = fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../samples")).unwrap();
    for dentry in paths {
        create_table_by_file(&ctx, dentry.unwrap().path())?;
    }
    Ok(ctx)
}

fn create_table_by_file<P: AsRef<Path>>(ctx: &SessionContext, path: P) -> Result<()> {
    let path = path.as_ref();
    let data = fs::read(path).unwrap();
    let resp: Response = serde_json::from_slice(&data).map_err(|e| {
        DataFusionError::Execution(format!("Failed to parse JSON file {}: {e}", path.display()))
    })?;
    // XXX-FIXME: collect labels from all time series, not only the first one
    let schema = Arc::new(create_schema_from_record(&resp.data.result[0]));
    let batch = create_record_batch(schema.clone(), &resp.data.result)?;
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    let table_name = resp.data.result[0].metric["__name__"].as_str().unwrap();
    ctx.register_table(table_name, Arc::new(provider))?;
    Ok(())
}

fn create_schema_from_record(data: &TimeSeries) -> Schema {
    let mut fields = data
        .metric
        .keys()
        .map(|k| Field::new(k, DataType::Utf8, true))
        .collect::<Vec<_>>();
    fields.push(Field::new("_timestamp", DataType::Int64, false));
    fields.push(Field::new("value", DataType::Float64, false));
    Schema::new(fields)
}

fn create_record_batch(schema: Arc<Schema>, data: &[TimeSeries]) -> Result<RecordBatch> {
    let mut field_values = HashMap::<_, Vec<_>>::new();
    let mut time_field_values = Vec::new();
    let mut value_field_values = Vec::new();

    for time_series in data {
        for sample in &time_series.values {
            for field in schema.fields() {
                let field_name = field.name();
                if field_name == "_timestamp" || field_name == "value" {
                    continue;
                }
                let field_value = time_series
                    .metric
                    .get(field_name)
                    .map(|v| v.as_str().unwrap())
                    .unwrap_or_default();
                field_values
                    .entry(field_name)
                    .or_default()
                    .push(field_value);
            }
            time_field_values.push(sample.timestamp * 1_000_000);
            value_field_values.push(sample.value.parse::<f64>().unwrap());
        }
    }

    let mut columns: Vec<ArrayRef> = Vec::new();
    for field in schema.fields() {
        let field_name = field.name();
        if field_name == "_timestamp" || field_name == "value" {
            continue;
        }
        let field_values = &field_values[field_name];
        let column = Arc::new(StringArray::from(field_values.clone()));
        columns.push(column);
    }
    columns.push(Arc::new(Int64Array::from(time_field_values)));
    columns.push(Arc::new(Float64Array::from(value_field_values)));

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

/// Prometheus HTTP API response
///
/// See https://prometheus.io/docs/prometheus/latest/querying/api/
#[derive(Debug, Serialize, Deserialize)]
struct Response {
    pub status: String,
    pub data: ResponseData,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResponseData {
    result_type: String,
    result: Vec<TimeSeries>,
}

/// See https://docs.victoriametrics.com/keyConcepts.html#time-series
#[derive(Debug, Serialize, Deserialize)]
struct TimeSeries {
    metric: serde_json::Map<String, serde_json::Value>,
    values: Vec<DataPoint>,
}

/// See https://docs.victoriametrics.com/keyConcepts.html#raw-samples
#[derive(Debug, Serialize, Deserialize)]
struct DataPoint {
    timestamp: i64,
    value: String,
}
