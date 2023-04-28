use arrow_array::{ArrayRef, Float64Array};
use clap::Parser;
use color_eyre::eyre::{eyre, Result, WrapErr};
use datafusion::{
    arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    datasource::MemTable,
    prelude::SessionContext,
};
use promql_parser::parser;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use newpromql::value::*;

mod api;
mod http;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(help = r#"PromQL expression

Examples:
    zo_http_incoming_requests
    zo_http_incoming_requests @ 1681713185
    irate(zo_response_time_count{cluster="zo1"}[5m])
    topk(1, irate(zo_response_time_count{cluster="zo1"}[5m]))
    histogram_quantile(0.9, rate(zo_response_time_bucket[5m]))"#)]
    expr: Option<String>,
    /// Run as HTTP API server
    #[arg(short, long, conflicts_with = "expr")]
    server: bool,
    /// Enable debug mode
    #[arg(short, long)]
    debug: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();
    let start_time = time::Instant::now();

    // read data updated timestamp
    let data_end = get_updated_timestamp()?;
    let data_start = data_end - 1800;
    tracing::info!(
        "loading data within time interval [ {} .. {} ]",
        data_start,
        data_end
    );

    let ctx = create_context()?;
    tracing::info!("prepare time: {}", start_time.elapsed());

    ctx.catalog_names().iter().for_each(|name| {
        tracing::info!("catalog: {}", name);
    });

    if cli.server {
        tracing::info!("start http server: {}", start_time.elapsed());
        http::server(ctx).await;
        tracing::info!("stopping http server: {}", start_time.elapsed());
        return Ok(());
    }

    let prom_expr = parser::parse(&cli.expr.unwrap()).map_err(|e| eyre!("parsing failed: {e}"))?;
    if cli.debug {
        dbg!(&prom_expr);
    }

    let eval_stmt = parser::EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH
            .checked_add(Duration::from_secs(data_start))
            .unwrap(),
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(data_end))
            .unwrap(),
        interval: Duration::from_secs(15), // step
        lookback_delta: Duration::from_secs(300),
    };

    let mut engine = newpromql::QueryEngine::new(Arc::new(ctx));
    let data = engine.exec(eval_stmt).await?;
    if cli.debug {
        dbg!(data);
    }
    tracing::info!("execute time: {}", start_time.elapsed());
    Ok(())
}

fn get_updated_timestamp() -> Result<u64> {
    let path = "samples/timestamp.log";
    match fs::read_to_string(path).wrap_err(path) {
        Ok(s) => Ok(s.trim().parse().wrap_err(path)?),
        Err(e) => {
            tracing::error!("load samples error: {:?}", e);
            Ok(0)
        }
    }
}

// create local session context with an in-memory table
fn create_context() -> Result<SessionContext> {
    let mut ctx = SessionContext::new();
    let dir = "samples";
    let paths = fs::read_dir(dir).wrap_err(dir)?;
    for dentry in paths {
        create_table_by_file(&ctx, dentry?.path())?;
    }
    // register regexp match
    newpromql::datafusion::register_udf(&mut ctx);
    Ok(ctx)
}

fn create_table_by_file<P: AsRef<Path>>(ctx: &SessionContext, path: P) -> Result<()> {
    let path = path.as_ref();
    let file_path = path.to_str().unwrap();
    if !file_path.ends_with(".json") {
        return Ok(());
    }
    let metric_type = match file_path {
        p if p.contains("counter") => TYPE_COUNTER,
        p if p.contains("gauge") => TYPE_GAUGE,
        p if p.contains("histogram_bucket") => TYPE_HISTOGRAM,
        p if p.contains("histogram_count") => TYPE_COUNTER,
        p if p.contains("histogram_sum") => TYPE_COUNTER,
        p if p.contains("summary") => TYPE_SUMMARY,
        _ => return Ok(()),
    };

    let data = fs::read(path).wrap_err_with(|| format!("{}", path.display()))?;
    let resp: Response = serde_json::from_slice(&data)
        .map_err(|e| eyre!("Failed to parse JSON file {}: {e}", path.display()))?;
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
        let hash_value = hash_value.as_str();

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
                let field_value = time_series
                    .metric
                    .get(field_name)
                    .map_or("", |v| v.as_str().unwrap());
                field_values
                    .entry(field_name.clone())
                    .or_default()
                    .push(field_value.to_string());
            }
            field_values
                .entry(FIELD_HASH.to_string())
                .or_default()
                .push(hash_value.to_string());
            field_values
                .entry(FIELD_TYPE.to_string())
                .or_default()
                .push(metric_type.to_string());
            time_field_values.push((sample.timestamp * 1_000_000.0) as i64);
            value_field_values.push(sample.value.parse::<f64>()?);
        }
    }

    let mut columns: Vec<ArrayRef> = Vec::new();
    for field in schema.fields() {
        let field_name = field.name();
        if field_name == FIELD_TIME || field_name == FIELD_VALUE {
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
    timestamp: f64,
    value: String,
}
