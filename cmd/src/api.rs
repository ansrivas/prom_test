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

use axum::{extract::Query, response::Json};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use promql_parser::parser;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct QueryRequest {
    pub query: String,
    pub time: Option<i64>,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub step: Option<i64>,
    pub timeout: Option<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResult {
    pub result_type: String, // vector, matrix, scalar, string
    pub result: newpromql::value::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    pub status: String, // success, error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<QueryResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub async fn query(req: Query<QueryRequest>) -> Json<QueryResponse> {
    let start_time = time::Instant::now();

    let prom_expr = match parser::parse(&req.query) {
        Ok(expr) => expr,
        Err(e) => {
            return Json(QueryResponse {
                status: "error".to_string(),
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

    let ctx = super::create_context().unwrap();
    tracing::info!("prepare time: {}", start_time.elapsed());

    let mut engine = newpromql::QueryEngine::new(ctx);
    let response = match engine.exec(eval_stmt).await {
        Ok(data) => QueryResponse {
            status: "success".to_string(),
            data: Some(QueryResult {
                result_type: data.get_type().to_string(),
                result: data,
            }),
            error_type: None,
            error: None,
        },
        Err(e) => QueryResponse {
            status: "error".to_string(),
            data: None,
            error_type: Some("bad_data".to_string()),
            error: Some(e.to_string()),
        },
    };
    tracing::info!("execute time: {}", start_time.elapsed());

    Json(response)
}
