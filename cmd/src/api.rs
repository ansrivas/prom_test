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
use serde_json::{json, Value};

#[derive(Deserialize, Serialize)]
pub struct QueryRequest {
    pub query: String,
    pub time: Option<i64>,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub step: Option<i64>,
    pub timeout: Option<i64>,
}

pub async fn query(req: Query<QueryRequest>) -> Json<Value> {
    let start_time = time::Instant::now();

    let prom_expr = parser::parse(&req.query).unwrap();

    let mut start = match req.start {
        Some(v) => UNIX_EPOCH
            .checked_add(Duration::from_secs(v as u64))
            .unwrap(),
        None => SystemTime::now(),
    };
    if req.time.is_some() {
        start = match req.time {
            Some(v) => UNIX_EPOCH
                .checked_add(Duration::from_secs(v as u64))
                .unwrap(),
            None => SystemTime::now(),
        };
    }
    let end = match req.end {
        Some(v) => UNIX_EPOCH
            .checked_add(Duration::from_secs(v as u64))
            .unwrap(),
        None => start,
    };
    let interval = match req.step {
        Some(v) => Duration::from_secs(v as u64),
        None => Duration::from_secs(300),
    };
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
    let data = engine.exec(eval_stmt).await.unwrap();
    tracing::info!("execute time: {}", start_time.elapsed());

    Json(json!(data))
}
