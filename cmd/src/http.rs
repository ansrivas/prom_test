use axum::routing::get;
use axum::Router;
use std::net::SocketAddr;
use tower_http::trace::{self, TraceLayer};
use tracing::Level;

use super::api;

pub async fn server() {
    // build our application with a route
    let app = Router::new()
        .route("/api/v1/query", get(api::query).post(api::query))
        .route("/api/v1/query_range", get(api::query).post(api::query))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                .on_response(trace::DefaultOnResponse::new().level(Level::INFO)),
        );

    // run our app with hyper
    let addr = SocketAddr::from(([0, 0, 0, 0], 5080));
    tracing::info!("start http server: {:?}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
