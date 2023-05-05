// Copyright 2022 Zinc Labs Inc. and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Query, State},
    response::Json,
};
use datafusion::prelude::SessionContext;
use promql_parser::parser;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Success,
    Error,
}

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub query: String,
    pub time: Option<i64>,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub step: Option<i64>,
    pub timeout: Option<i64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResult {
    pub result_type: String, // vector, matrix, scalar, string
    pub result: newpromql::value::Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<QueryResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MetadataRequest {
    /// Maximum number of metrics to return.
    pub limit: Option<usize>,
    /// A metric name to filter metadata for. All metric metadata is retrieved
    /// if left empty.
    pub metric: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct MetadataResponse {
    pub status: Status,
    /// key - metric name
    pub data: HashMap<String, Vec<MetadataObject>>,
}

#[derive(Debug, Serialize)]
pub struct MetadataObject {
    #[serde(rename = "type")]
    typ: String, // counter, gauge, histogram, summary
    help: String,
    unit: String,
}

pub async fn index() -> String {
    "Hello, World!".to_string()
}

pub async fn query(
    req: Query<QueryRequest>,
    State(ctx): State<Arc<SessionContext>>,
) -> Json<QueryResponse> {
    // let start_time = time::Instant::now();

    let prom_expr = match parser::parse(&req.query) {
        Ok(expr) => expr,
        Err(e) => {
            return Json(QueryResponse {
                status: Status::Error,
                data: None,
                error_type: Some("bad_data".to_string()),
                error: Some(e),
            })
        }
    };

    let mk_time = |t| {
        UNIX_EPOCH
            .checked_add(Duration::from_secs(t as u64))
            .unwrap()
    };
    let start = if let Some(t) = req.time {
        mk_time(t)
    } else {
        req.start.map_or_else(SystemTime::now, mk_time)
    };
    let end = req.end.map_or(start, mk_time);
    let interval = Duration::from_secs(req.step.map_or(300, |t| t as u64));

    let eval_stmt = parser::EvalStmt {
        expr: prom_expr,
        start,
        end,
        interval, // step
        lookback_delta: Duration::from_secs(300),
    };

    let mut engine = newpromql::QueryEngine::new(ctx);
    let response = match engine.exec(eval_stmt).await {
        Ok(data) => QueryResponse {
            status: Status::Success,
            data: Some(QueryResult {
                result_type: data.get_type().to_string(),
                result: data,
            }),
            error_type: None,
            error: None,
        },
        Err(e) => QueryResponse {
            status: Status::Error,
            data: None,
            error_type: Some("bad_data".to_string()),
            error: Some(e.to_string()),
        },
    };
    // tracing::info!("execute time: {}", start_time.elapsed());

    Json(response)
}

pub async fn metadata(
    req: Query<MetadataRequest>,
    State(ctx): State<Arc<SessionContext>>,
) -> Json<MetadataResponse> {
    let catalog = ctx.catalog("datafusion").unwrap();
    let schema = catalog.schema("public").unwrap();
    let table_names = schema.table_names().into_iter().map(|name| {
        (
            name,
            vec![
                // XXX-TODO: fill with `MetadataObject`s
            ],
        )
    });
    Json(MetadataResponse {
        status: Status::Success,
        data: req.limit.map_or(table_names.clone().collect(), |limit| {
            table_names.take(limit).collect()
        }),
    })
}
