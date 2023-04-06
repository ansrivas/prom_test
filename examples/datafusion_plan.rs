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
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::prelude::*;

use datafusion::error::Result;
use datafusion_expr::LogicalPlanBuilder;
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
    let file_format = ParquetFormat::default().with_enable_pruning(Some(false));
    let listing_options =
        ListingOptions::new(Arc::new(file_format)).with_file_extension(FileType::PARQUET.get_ext());
    let prefix = match ListingTableUrl::parse(&format!("file://{testdata}")) {
        Ok(url) => url,
        Err(e) => {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "ListingTableUrl error: {e}"
            )));
        }
    };
    let prefixes = vec![prefix];

    let mut config =
        ListingTableConfig::new_with_multi_paths(prefixes).with_listing_options(listing_options);
    config = config.infer_schema(&ctx.state()).await.unwrap();

    let table = ListingTable::try_new(config)?;
    let table = Arc::new(table);
    let schema = table.schema();

    // Register a listing table - this will use all files in the directory as data sources
    // for the query
    ctx.register_table("tbl", table.clone())?;
    log::info!(
        "Prepare table took {:.3} seconds.",
        start.elapsed().as_secs_f64()
    );

    // execute SQL
    let sql = "SELECT kubernetes_namespace_name, count(*) as num FROM tbl WHERE kubernetes_namespace_name = 'ziox-noderole' GROUP BY kubernetes_namespace_name LIMIT 100";
    let df = ctx.sql(sql).await.unwrap();
    log::info!(
        "Query prepare {:.3} seconds.",
        start.elapsed().as_secs_f64()
    );
    df.show().await?;
    log::info!("Query took {:.3} seconds.", start.elapsed().as_secs_f64());

    // create dataframe
    let df = ctx.table("tbl").await.unwrap();
    let df = df
        .filter(col("kubernetes_namespace_name").eq(lit("ziox-noderole")))?
        .aggregate(
            vec![col("kubernetes_namespace_name")],
            vec![count(lit("*")).alias("num")],
        )?
        .select(vec![col("kubernetes_namespace_name")])?
        .limit(0, Some(100))?;
    log::info!(
        "Query prepare {:.3} seconds.",
        start.elapsed().as_secs_f64()
    );
    df.clone().show().await?;
    log::info!("Query took {:.3} seconds.", start.elapsed().as_secs_f64());

    // test logical plan
    let logical_plan = df.clone().into_unoptimized_plan();
    log::info!("Unoptimized plan:\n{:?}", logical_plan.clone());
    let logical_plan = df.clone().into_optimized_plan()?;
    log::info!("Optimized plan:\n{:?}", logical_plan.clone());

    let field_id = schema
        .fields()
        .iter()
        .position(|v| v.name() == "kubernetes_namespace_name");

    let ctx_table = ctx.table_provider("tbl").await?;
    let table_scan_plan = LogicalPlanBuilder::scan_with_filters(
        "tbl",
        Arc::new(DefaultTableSource::new(ctx_table)),
        Some(vec![field_id.unwrap()]),
        vec![col("kubernetes_namespace_name").eq(lit("ziox-noderole"))],
    )
    .unwrap();
    let filter_plan = LogicalPlanBuilder::filter(
        table_scan_plan,
        col("kubernetes_namespace_name").eq(lit("ziox-noderole")),
    )
    .unwrap();
    let aggregate_plan = LogicalPlanBuilder::aggregate(
        filter_plan,
        vec![col("kubernetes_namespace_name")],
        vec![count(lit("*")).alias("num")],
    )
    .unwrap();
    let projection_plan =
        LogicalPlanBuilder::project(aggregate_plan, vec![col("kubernetes_namespace_name")])
            .unwrap();
    let limit_plan = LogicalPlanBuilder::limit(projection_plan, 0, Some(100)).unwrap();
    let logical_plan = limit_plan.build().unwrap();
    log::info!("customed plan:\n{:?}", logical_plan.clone());

    let logical_plan = ctx.state().optimize(&logical_plan)?;
    log::info!("optimized plan:\n{:?}", logical_plan.clone());

    let execute_plan = ctx.state().create_physical_plan(&logical_plan).await?;
    log::info!(
        "Query prepare {:.3} seconds.",
        start.elapsed().as_secs_f64()
    );
    let result = datafusion::physical_plan::collect(execute_plan, ctx.task_ctx()).await?;
    let pretty_print = datafusion::arrow::util::pretty::pretty_format_batches(&result).unwrap();
    println!("{}", pretty_print);

    // print the results
    // df.show().await?;
    log::info!("Query took {:.3} seconds.", start.elapsed().as_secs_f64());

    Ok(())
}
