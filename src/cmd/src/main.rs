use arrow_array::{ArrayRef, Float64Array};
use clap::Parser;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion::{
    arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    error::DataFusionError,
};
use promql_parser::parser::{self, EvalStmt};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::prelude::*;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

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
    let cli = Cli::parse();
    let start_time = time::Instant::now();
    let prom_expr = parser::parse(&cli.expr).unwrap();

    let eval_stmt = EvalStmt {
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
    eprintln!("prepare time: {}", start_time.elapsed());

    let mut engine = newpromql::QueryEngine::new(ctx);
    let data = engine.exec(eval_stmt).await.unwrap();
    dbg!(data);
    eprintln!("execute time: {}", start_time.elapsed());
}

// create local session context with an in-memory table
fn create_context() -> Result<SessionContext> {
    let data_path = "./samples/";
    let ctx = SessionContext::new();

    let paths = fs::read_dir(data_path).unwrap();
    for path in paths {
        create_table_by_file(&ctx, path.unwrap().path().to_str().unwrap()).unwrap();
    }

    Ok(ctx)
}

fn create_table_by_file<'a>(ctx: &'a SessionContext, path: &'a str) -> Result<()> {
    let data = load_file_contents(path)?;
    let values: MetricResult = match serde_json::from_str(&data) {
        Ok(v) => v,
        Err(e) => {
            return Err(DataFusionError::Execution(format!(
                "Failed to parse json file: {}",
                e
            )));
        }
    };

    let table_name = get_table_name(&values)?;
    let schema = create_schema_from_record(&values.data.result[0])?;
    let schema = Arc::new(schema);
    let batch = create_record_batch(schema.clone(), &values.data.result)?;
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table(table_name.as_str(), Arc::new(provider))?;
    Ok(())
}

fn get_table_name(data: &MetricResult) -> Result<String> {
    let metric = data.data.result[0].metric.clone();
    let name = metric.get("__name__").unwrap().as_str().unwrap();
    Ok(name.to_string())
}

fn create_schema_from_record(data: &MetricResultItem) -> Result<Schema> {
    let mut fields = Vec::new();
    let metric = data.metric.clone();
    for (key, _value) in metric {
        let field = Field::new(&key, DataType::Utf8, true);
        fields.push(field);
    }
    fields.push(Field::new("_timestamp", DataType::Int64, false));
    fields.push(Field::new("value", DataType::Float64, false));
    Ok(Schema::new(fields))
}

fn create_record_batch(schema: Arc<Schema>, data: &[MetricResultItem]) -> Result<RecordBatch> {
    let fields = schema.fields();
    let mut fileds_values = HashMap::new();
    let mut time_field_values = Vec::new();
    let mut value_field_values = Vec::new();
    for value in data {
        for val in &value.values {
            for field in fields {
                let field_name = field.name();
                if field_name == "_timestamp" || field_name == "value" {
                    continue;
                }
                let field_value = match value.metric.get(field_name) {
                    Some(v) => v.as_str().unwrap(),
                    None => "",
                };
                let field_values = fileds_values.entry(field_name).or_insert(Vec::new());
                field_values.push(field_value);
            }
            time_field_values.push(val.timestamp * 1_000_000);
            value_field_values.push(val.value.parse::<f64>().unwrap());
        }
    }

    let mut columns: Vec<ArrayRef> = Vec::new();
    for field in fields {
        let field_name = field.name();
        if field_name == "_timestamp" || field_name == "value" {
            continue;
        }
        let field_values = fileds_values.get(field_name).unwrap();
        let column = Arc::new(StringArray::from(field_values.clone()));
        columns.push(column);
    }
    columns.push(Arc::new(Int64Array::from(time_field_values)));
    columns.push(Arc::new(Float64Array::from(value_field_values)));

    let batch = RecordBatch::try_new(schema.clone(), columns.to_vec())?;
    Ok(batch)
}

fn load_file_contents(path: &str) -> Result<String> {
    let mut file = File::open(path).expect("Failed to open file");
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("Could not read file");
    Ok(contents)
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct MetricResult {
    pub status: String,
    pub data: MetricResultData,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct MetricResultData {
    #[serde(rename = "resultType")]
    pub result_type: String,
    pub result: Vec<MetricResultItem>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct MetricResultItem {
    pub metric: Map<String, Value>,
    pub values: Vec<MetricResultValue>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct MetricResultValue {
    pub timestamp: i64,
    pub value: String,
}
