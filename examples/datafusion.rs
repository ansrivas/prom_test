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

use datafusion::datasource::file_format::file_type::{FileType, GetExt};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable};
use datafusion::datasource::listing::{ListingTableConfig, ListingTableUrl};
use datafusion::datasource::object_store::DefaultObjectStoreRegistry;
use datafusion::error::Result;
use datafusion::execution::context::SessionConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    match sql().await {
        Ok(_) => {
            println!("done");
        }
        Err(e) => {
            println!("error: {:?}", e);
        }
    }
    Ok(())
}

async fn sql() -> Result<()> {
    let start = Instant::now();
    let runtime_env = create_runtime_env()?;
    let session_config = SessionConfig::new()
        .with_information_schema(true)
        .with_batch_size(8192);
    let ctx = SessionContext::with_config_rt(session_config.clone(), Arc::new(runtime_env));

    // Configure listing options
    let listing_options = {
        let file_format = ParquetFormat::default().with_enable_pruning(Some(false));
        ListingOptions::new(Arc::new(file_format)).with_file_extension(FileType::PARQUET.get_ext())
    };

    let prefix = "/Users/yanghengfei/code/rust/github.com/zinclabs/zinc-observe/tmp/parquets/files/default/logs/k8s/";
    let prefix = match ListingTableUrl::parse(prefix) {
        Ok(url) => url,
        Err(e) => {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "ListingTableUrl error: {e}",
            )));
        }
    };
    // let prefixes = vec![prefix];
    log::info!(
        "Prepare table took {:.3} seconds.",
        start.elapsed().as_secs_f64()
    );

    let config = ListingTableConfig::new(prefix)
        .with_listing_options(listing_options)
        .infer_schema(&ctx.state())
        .await
        .unwrap();
    log::info!(
        "infer schema took {:.3} seconds.",
        start.elapsed().as_secs_f64()
    );

    let table = ListingTable::try_new(config)?;
    ctx.register_table("tbl", Arc::new(table))?;

    log::info!(
        "Register table took {:.3} seconds.",
        start.elapsed().as_secs_f64()
    );

    // Debug SQL
    let origin_sql = "select * FROM tbl WHERE (_timestamp >= 1680310305287000 AND _timestamp < 1680396705287000)  ORDER BY _timestamp DESC LIMIT 10";
    log::info!("Query sql: {}", origin_sql);

    // query
    let df = match ctx.sql(&origin_sql).await {
        Ok(df) => df,
        Err(e) => {
            log::error!(
                "query sql execute failed, sql: {}, err: {:?}",
                origin_sql,
                e
            );
            return Err(e);
        }
    };

    // let batches = df.collect().await?;
    df.show().await?;
    log::info!("Query took {:.3} seconds.", start.elapsed().as_secs_f64());

    // drop table
    ctx.deregister_table("tbl")?;
    log::info!(
        "Query all took {:.3} seconds.",
        start.elapsed().as_secs_f64()
    );

    Ok(())
}

fn create_runtime_env() -> Result<RuntimeEnv> {
    let object_store_registry = DefaultObjectStoreRegistry::new();

    let rn_config =
        RuntimeConfig::new().with_object_store_registry(Arc::new(object_store_registry));
    RuntimeEnv::new(rn_config)
}
