use std::{
    collections::{HashMap, HashSet},
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
use newpromql::value::*;
use promql_parser::parser;
use serde::{Deserialize, Serialize};

mod api;
mod http;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(help = r#"PromQL expression

Examples:
    irate(zo_response_time_count{cluster="zo1"}[5m])
    topk(1, irate(zo_response_time_count{cluster="zo1"}[5m]))
    histogram_quantile(0.9, rate(zo_response_time_bucket[5m]))"#)]
    expr: String,
    #[arg(short, long, help = "debug mode")]
    debug: bool,
    #[arg(short, long, help = "server mode")]
    server: bool,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();
    let cli = Cli::parse();
    let start_time = time::Instant::now();

    if cli.server {
        tracing::info!("start http server: {}", start_time.elapsed());
        http::server().await;
        tracing::info!("stopping http server: {}", start_time.elapsed());
        return;
    }

    let prom_expr = parser::parse(&cli.expr).unwrap();
    if cli.debug {
        dbg!(&prom_expr);
    }
    std::fs::write("/tmp/XXX.ast.rs", format!("{prom_expr:#?}\n")).unwrap(); // XXX-DELETEME

    let eval_stmt = parser::EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH
            .checked_add(Duration::from_secs(1681711400))
            .unwrap(),
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(1681711400)) // 2 minutes 1681711520 // 30 minutes 1681713200
            .unwrap(),
        interval: Duration::from_secs(15), // step
        lookback_delta: Duration::from_secs(300),
    };

    let ctx = create_context().unwrap();
    tracing::info!("prepare time: {}", start_time.elapsed());

    let mut engine = newpromql::QueryEngine::new(ctx);
    let data = engine.exec(eval_stmt).await.unwrap();
    if cli.debug {
        dbg!(data);
    }
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
    let metric_type = match path {
        p if p.ends_with("counter.json") => "counter",
        p if p.ends_with("gauge.json") => "gauge",
        p if p.ends_with("histogram_bucket.json") => "histogram",
        p if p.ends_with("histogram_count.json") => "counter",
        p if p.ends_with("histogram_sum.json") => "counter",
        p if p.ends_with("summary.json") => "summary",
        _ => "",
    };
    let data = fs::read(path).unwrap();
    let resp: Response = serde_json::from_slice(&data).map_err(|e| {
        DataFusionError::Execution(format!("Failed to parse JSON file {}: {e}", path.display()))
    })?;
    // XXX-FIXME: collect labels from all time series, not only the first one
    let schema = Arc::new(create_schema_from_record(&resp.data.result));
    let batch = create_record_batch(metric_type, schema.clone(), &resp.data.result)?;
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    let table_name = resp.data.result[0].metric["__name__"].as_str().unwrap();
    ctx.register_table(table_name, Arc::new(provider))?;
    Ok(())
}

fn create_schema_from_record(data: &[TimeSeries]) -> Schema {
    let mut fields_map = HashSet::new();
    let mut fields = Vec::new();
    for row in data {
        row.metric.keys().for_each(|k| {
            if !fields_map.contains(k) {
                fields_map.insert(k.to_string());
                fields.push(Field::new(k, DataType::Utf8, true));
            }
        });
    }
    fields.push(Field::new(FIELD_HASH.to_string(), DataType::Utf8, false));
    fields.push(Field::new(FIELD_TYPE.to_string(), DataType::Utf8, false));
    fields.push(Field::new(FIELD_TIME.to_string(), DataType::Int64, false));
    fields.push(Field::new(
        FIELD_VALUE.to_string(),
        DataType::Float64,
        false,
    ));
    Schema::new(fields)
}

fn create_record_batch(
    metric_type: &str,
    schema: Arc<Schema>,
    data: &[TimeSeries],
) -> Result<RecordBatch> {
    let mut field_values = HashMap::<_, Vec<_>>::new();
    let mut time_field_values = Vec::new();
    let mut value_field_values = Vec::new();

    for time_series in data {
        let mut field_map = HashMap::new();
        time_series.metric.iter().for_each(|(k, v)| {
            field_map.insert(k.to_string(), v.to_string());
        });
        let hash_value = signature(&field_map);

        for sample in &time_series.values {
            for field in schema.fields() {
                let field_name = field.name();
                if field_name == FIELD_HASH
                    || field_name == FIELD_TYPE
                    || field_name == FIELD_TIME
                    || field_name == FIELD_VALUE
                {
                    continue;
                }
                let field_value = match time_series.metric.get(field_name) {
                    Some(v) => v.as_str().unwrap(),
                    None => "",
                };
                field_values
                    .entry(field_name.to_string())
                    .or_default()
                    .push(field_value.to_string());
            }
            field_values
                .entry(FIELD_HASH.to_string())
                .or_default()
                .push(hash_value.clone());
            field_values
                .entry(FIELD_TYPE.to_string())
                .or_default()
                .push(metric_type.to_string());
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

    Ok(RecordBatch::try_new(schema, columns)?)
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
    values: Vec<Sample>,
}

/// See https://docs.victoriametrics.com/keyConcepts.html#raw-samples
#[derive(Debug, Serialize, Deserialize)]
struct Sample {
    timestamp: i64,
    value: String,
}
