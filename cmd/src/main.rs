use clap::Parser;
use color_eyre::eyre::{eyre, Result};
use promql_parser::parser;
use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

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

    let (data_start, data_end) = prom_test::load_timestamps("samples")?;
    tracing::info!(
        "loading data within time interval [ {} .. {} ]",
        data_start,
        data_end
    );

    let ctx = prom_test::create_context("samples")?;
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
