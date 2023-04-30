use arrow_array::{ArrayRef, Float64Array};
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
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use newpromql::value;

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

// Creates local session context with an in-memory table.
pub fn create_context(samples_dir: impl AsRef<Path>) -> Result<SessionContext> {
    let mut ctx = SessionContext::new();
    let samples_dir = samples_dir.as_ref();
    let paths = fs::read_dir(samples_dir).wrap_err_with(|| format!("{}", samples_dir.display()))?;
    for dentry in paths {
        create_table_by_file(&ctx, dentry?.path())?;
    }
    newpromql::datafusion::register_udf(&mut ctx); // register regexp match
    Ok(ctx)
}

/// Obtains start and end timestamps from `samples/timestamp.log` file.
pub fn load_timestamps(samples_dir: impl AsRef<Path>) -> Result<(u64, u64)> {
    let path = samples_dir.as_ref().join("timestamp.log");
    let end = match fs::read_to_string(&path) {
        Ok(s) => s.trim().parse().wrap_err_with(|| {
            format!("failed to read Unix time (epoch) from {}", path.display())
        })?,
        Err(error) => {
            tracing::error!(?error, path = %path.display(), "failed to read file");
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        }
    };
    Ok((end - 1800, end))
}

fn create_table_by_file<P: AsRef<Path>>(ctx: &SessionContext, path: P) -> Result<()> {
    let path = path.as_ref();
    let file_path = path.to_str().unwrap();
    if !file_path.ends_with(".json") {
        return Ok(());
    }
    let metric_type = match file_path {
        p if p.contains("counter") => value::TYPE_COUNTER,
        p if p.contains("gauge") => value::TYPE_GAUGE,
        p if p.contains("histogram_bucket") => value::TYPE_HISTOGRAM,
        p if p.contains("histogram_count") => value::TYPE_COUNTER,
        p if p.contains("histogram_sum") => value::TYPE_COUNTER,
        p if p.contains("summary") => value::TYPE_SUMMARY,
        _ => return Ok(()),
    };

    let data = fs::read(path).wrap_err_with(|| format!("{}", path.display()))?;
    let resp: Response = serde_json::from_slice(&data)
        .map_err(|e| eyre!("Failed to parse JSON file {}: {e}", path.display()))?;
    let schema = Arc::new(create_schema_from_record(&resp.data.result));
    let batch = create_record_batch(metric_type, schema.clone(), &resp.data.result)?;
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    let table_name = resp.data.result[0].metric["__name__"].as_str().unwrap();
    ctx.register_table(table_name, Arc::new(provider))?;
    Ok(())
}

fn create_schema_from_record(data: &[TimeSeries]) -> Schema {
    let mut fields_map = FxHashSet::default();
    let mut fields = Vec::new();
    for row in data {
        row.metric.keys().for_each(|k| {
            if !fields_map.contains(k) {
                fields_map.insert(k.to_string());
                fields.push(Field::new(k, DataType::Utf8, true));
            }
        });
    }
    fields.push(Field::new(
        value::FIELD_HASH.to_string(),
        DataType::Utf8,
        false,
    ));
    fields.push(Field::new(
        value::FIELD_TYPE.to_string(),
        DataType::Utf8,
        false,
    ));
    fields.push(Field::new(
        value::FIELD_TIME.to_string(),
        DataType::Int64,
        false,
    ));
    fields.push(Field::new(
        value::FIELD_VALUE.to_string(),
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
    let mut field_values = FxHashMap::<_, Vec<_>>::default();
    let mut time_field_values = Vec::new();
    let mut value_field_values = Vec::new();

    for time_series in data {
        let mut field_map = FxHashMap::default();
        time_series.metric.iter().for_each(|(k, v)| {
            field_map.insert(k.to_string(), v.to_string());
        });
        let hash_value = value::signature(&field_map);

        for sample in &time_series.values {
            for field in schema.fields() {
                let field_name = field.name();
                if field_name == value::FIELD_HASH
                    || field_name == value::FIELD_TYPE
                    || field_name == value::FIELD_TIME
                    || field_name == value::FIELD_VALUE
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
                .entry(value::FIELD_HASH.to_string())
                .or_default()
                .push(hash_value.clone().into());
            field_values
                .entry(value::FIELD_TYPE.to_string())
                .or_default()
                .push(metric_type.to_string());
            time_field_values.push((sample.timestamp * 1_000_000.0) as i64);
            value_field_values.push(sample.value.parse::<f64>()?);
        }
    }

    let mut columns: Vec<ArrayRef> = Vec::new();
    for field in schema.fields() {
        let field_name = field.name();
        if field_name == value::FIELD_TIME || field_name == value::FIELD_VALUE {
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
