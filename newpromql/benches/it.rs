use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use promql_parser::parser;
use tokio::runtime::Runtime;

fn bench_promql(c: &mut Criterion) {
    let samples_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../samples");
    let (start, end) = prom_test::load_timestamps(samples_dir).unwrap();
    let ctx = Arc::new(prom_test::create_context(samples_dir).unwrap());
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("promql");
    group.measurement_time(Duration::from_secs(8));

    for (alias, query) in [
        (
            "rate",
            r#"sum by(endpoint) (rate(zo_http_incoming_requests{namespace="ziox-alpha1",organization="default"}[5m]))"#,
        ),
        (
            "irate",
            r#"sum by(endpoint) (irate(zo_http_incoming_requests{namespace="ziox-alpha1",organization="default"}[5m]))"#,
        ),
        (
            "increase",
            r#"sum by(endpoint) (increase(zo_http_incoming_requests{namespace="ziox-alpha1",organization="default"}[5m]))"#,
        ),
        (
            "delta",
            r#"sum by(stream_type) (delta(zo_storage_files{namespace="ziox-alpha1",organization="default"}[5m]))"#,
        ),
    ] {
        let eval_stmt = parser::EvalStmt {
            expr: parser::parse(query).unwrap(),
            start: UNIX_EPOCH.checked_add(Duration::from_secs(start)).unwrap(),
            end: UNIX_EPOCH.checked_add(Duration::from_secs(end)).unwrap(),
            interval: Duration::from_secs(15), // step
            lookback_delta: Duration::from_secs(300),
        };
        group.bench_function(BenchmarkId::from_parameter(format!("{alias}")), |b| {
            b.to_async(&rt).iter(|| async {
                let mut engine = newpromql::Query::new(Arc::clone(&ctx));
                let data = engine.exec(black_box(eval_stmt.clone())).await.unwrap();
                black_box(data);
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_promql);
criterion_main!(benches);
