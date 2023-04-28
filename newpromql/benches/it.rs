use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use promql_parser::parser;
use tokio::runtime::Runtime;

fn bench_promql(c: &mut Criterion) {
    let query = r#"zo_http_incoming_requests{namespace="ziox-alpha1",organization="default"}[35m]"#;

    let samples_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../samples");
    let (start, end) = prom_test::load_timestamps(samples_dir).unwrap();
    // REVIEW: do we want to measure PromQL parsing also?
    let prom_expr = parser::parse(query).unwrap();
    let eval_stmt = parser::EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH.checked_add(Duration::from_secs(start)).unwrap(),
        end: UNIX_EPOCH.checked_add(Duration::from_secs(end)).unwrap(),
        interval: Duration::from_secs(15), // step
        lookback_delta: Duration::from_secs(300),
    };

    let ctx = Arc::new(prom_test::create_context(samples_dir).unwrap());
    let rt = Runtime::new().unwrap();
    c.bench_function("promql", |b| {
        b.to_async(&rt).iter(|| async {
            let mut engine = newpromql::QueryEngine::new(Arc::clone(&ctx));
            let data = engine.exec(black_box(eval_stmt.clone())).await.unwrap();
            black_box(data);
        })
    });
}

criterion_group!(benches, bench_promql);
criterion_main!(benches);
