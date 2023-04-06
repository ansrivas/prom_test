// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion::datasource::file_format::file_type::{FileType, GetExt};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::physical_plan::collect;
use datafusion::prelude::*;

use datafusion::error::Result;
use std::sync::Arc;

/// This example demonstrates executing a simple query against an Arrow data source (a directory
/// with multiple Parquet files) and fetching results
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let start = std::time::Instant::now();
    // create local execution context
    let ctx = SessionContext::new();

    let testdata = "/Users/yanghengfei/code/rust/github.com/zinclabs/zinc-observe/tmp/parquets/files/default/logs/k8s/";

    // Configure listing options
    let file_format = ParquetFormat::default().with_enable_pruning(Some(true));
    let listing_options =
        ListingOptions::new(Arc::new(file_format)).with_file_extension(FileType::PARQUET.get_ext());

    // Register a listing table - this will use all files in the directory as data sources
    // for the query
    ctx.register_listing_table(
        "tbl",
        &format!("file://{testdata}"),
        listing_options,
        None,
        None,
    )
    .await
    .unwrap();
    log::info!(
        "Prepare table took {:.3} seconds.",
        start.elapsed().as_secs_f64()
    );

    // execute the query
    let df = ctx.table("tbl").await.unwrap();
    let df = df
        .filter(col("kubernetes_namespace_name").eq(lit("ziox-noderole")))?
        .aggregate(vec![col("kubernetes_namespace_name")], vec![count(lit("*"))])?
        .limit(0, Some(100))?;
    log::info!("Query took {:.3} seconds.", start.elapsed().as_secs_f64());

    let logical_plan = df.into_optimized_plan()?;
    // log::info!("Optimized plan:\n{:?}", logical_plan);
    let execute_plan = ctx.state().create_physical_plan(&logical_plan).await?;
    let result = collect(execute_plan, ctx.task_ctx()).await?;
    println!("result: {:?}", result);

    // print the results
    // df.show().await?;
    log::info!("Query took {:.3} seconds.", start.elapsed().as_secs_f64());

    Ok(())
}
