use promql_parser::parser;

fn main() {
    let promql = r#"avg(irate(zo_http_incoming_requests{namespace="ziox-dev"}[5m])) by (exported_endpoint)"#;

    match parser::parse(promql) {
        Ok(ast) => println!("AST: {:?}", ast),
        Err(info) => println!("Err: {:?}", info),
    }
}
