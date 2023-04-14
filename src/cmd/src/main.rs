use datafusion::arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use promql_parser::parser::{self, EvalStmt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct Results {
    pub metric: ResultMetric,
    pub values: Vec<ResultValue>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct ResultMetric {
    pub __name__: String,
    pub cluster: String,
    pub container: String,
    pub endpoint: String,
    pub exported_endpoint: String,
    pub instance: String,
    pub job: String,
    pub namespace: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct ResultValue {
    pub timestamp: i64,
    pub value: String,
}

static ORIGIN_VALUE: &str = r#"[{"metric":{"__name__":"zo_http_incoming_requests","cluster":"zo1","container":"zincobserve","endpoint":"http","exported_endpoint":"/_json","exported_instance":"zo1-zincobserve-ingester-0","instance":"10.24.0.164:5080","job":"zo1-zincobserve-ingester","namespace":"ziox-dev","organization":"default","pod":"zo1-zincobserve-ingester-0","role":"ingester","service":"zo1-zincobserve-ingester","status":"200","stream":"default","stream_type":"logs"},"values":[[1680161100,"890386"],[1680161115,"890513"],[1680161130,"890643"],[1680161145,"890773"],[1680161160,"890904"],[1680161175,"891031"],[1680161190,"891165"],[1680161205,"891291"],[1680161220,"891419"],[1680161235,"891548"],[1680161250,"891677"],[1680161265,"891805"],[1680161280,"891935"],[1680161295,"892061"],[1680161310,"892191"],[1680161325,"892317"],[1680161340,"892449"],[1680161355,"892572"],[1680161370,"892702"],[1680161385,"892834"],[1680161400,"892963"],[1680161415,"893095"],[1680161430,"893225"],[1680161445,"893353"],[1680161460,"893481"],[1680161475,"893611"],[1680161490,"893742"],[1680161505,"893873"],[1680161520,"894000"],[1680161535,"894126"],[1680161550,"894255"],[1680161565,"894383"],[1680161580,"894514"],[1680161595,"894641"],[1680161610,"894775"],[1680161625,"894903"],[1680161640,"895032"],[1680161655,"895161"],[1680161670,"895286"],[1680161685,"895416"],[1680161700,"895549"],[1680161715,"895673"],[1680161730,"895803"],[1680161745,"895929"],[1680161760,"896064"],[1680161775,"896189"],[1680161790,"896323"],[1680161805,"896449"],[1680161820,"896579"],[1680161835,"896702"],[1680161850,"896832"],[1680161865,"896962"],[1680161880,"897121"],[1680161895,"897267"],[1680161910,"897426"],[1680161925,"897586"],[1680161940,"897750"],[1680161955,"897910"],[1680161970,"898072"],[1680161985,"898210"],[1680162000,"898338"],[1680162015,"898467"],[1680162030,"898599"],[1680162045,"898730"],[1680162060,"898859"],[1680162075,"898988"],[1680162090,"899120"],[1680162105,"899248"],[1680162120,"899374"],[1680162135,"899495"],[1680162150,"899605"],[1680162165,"899725"],[1680162180,"899842"],[1680162195,"899975"],[1680162210,"900108"],[1680162225,"900234"],[1680162240,"900360"],[1680162255,"900484"],[1680162270,"900614"],[1680162285,"900745"],[1680162300,"900874"],[1680162315,"900998"],[1680162330,"901129"],[1680162345,"901260"],[1680162360,"901390"],[1680162375,"901518"],[1680162390,"901645"],[1680162405,"901778"],[1680162420,"901905"],[1680162435,"902031"],[1680162450,"902161"],[1680162465,"902291"],[1680162480,"902420"],[1680162495,"902548"],[1680162510,"902675"],[1680162525,"902802"],[1680162540,"902934"],[1680162555,"903060"],[1680162570,"903189"],[1680162585,"903318"],[1680162600,"903450"],[1680162615,"903573"],[1680162630,"903684"],[1680162645,"903802"],[1680162660,"903921"],[1680162675,"904052"],[1680162690,"904180"],[1680162705,"904314"],[1680162720,"904441"],[1680162735,"904566"],[1680162750,"904698"],[1680162765,"904827"],[1680162780,"904957"],[1680162795,"905082"],[1680162810,"905212"],[1680162825,"905339"],[1680162840,"905468"],[1680162855,"905593"],[1680162870,"905722"],[1680162885,"905853"],[1680162900,"905981"],[1680162915,"906108"],[1680162930,"906234"],[1680162945,"906364"],[1680162960,"906495"],[1680162975,"906623"],[1680162990,"906754"],[1680163005,"906881"],[1680163020,"907010"],[1680163035,"907140"],[1680163050,"907269"],[1680163065,"907398"],[1680163080,"907527"],[1680163095,"907654"],[1680163110,"907782"],[1680163125,"907914"],[1680163140,"908041"],[1680163155,"908169"],[1680163170,"908295"],[1680163185,"908425"],[1680163200,"908558"]]},{"metric":{"__name__":"zo_http_incoming_requests","cluster":"zo1","container":"zincobserve","endpoint":"http","exported_endpoint":"/_bulk","exported_instance":"zo1-zincobserve-ingester-0","instance":"10.24.0.164:5080","job":"zo1-zincobserve-ingester","namespace":"ziox-dev","organization":"default","pod":"zo1-zincobserve-ingester-0","role":"ingester","service":"zo1-zincobserve-ingester","status":"200","stream":"default","stream_type":"logs"},"values":[[1680161100,"890386"],[1680161115,"890513"],[1680161130,"890643"],[1680161145,"890773"],[1680161160,"890904"],[1680161175,"891031"],[1680161190,"891165"],[1680161205,"891291"],[1680161220,"891419"],[1680161235,"891548"],[1680161250,"891677"],[1680161265,"891805"],[1680161280,"891935"],[1680161295,"892061"],[1680161310,"892191"],[1680161325,"892317"],[1680161340,"892449"],[1680161355,"892572"],[1680161370,"892702"],[1680161385,"892834"],[1680161400,"892963"],[1680161415,"893095"],[1680161430,"893225"],[1680161445,"893353"],[1680161460,"893481"],[1680161475,"893611"],[1680161490,"893742"],[1680161505,"893873"],[1680161520,"894000"],[1680161535,"894126"],[1680161550,"894255"],[1680161565,"894383"],[1680161580,"894514"],[1680161595,"894641"],[1680161610,"894775"],[1680161625,"894903"],[1680161640,"895032"],[1680161655,"895161"],[1680161670,"895286"],[1680161685,"895416"],[1680161700,"895549"],[1680161715,"895673"],[1680161730,"895803"],[1680161745,"895929"],[1680161760,"896064"],[1680161775,"896189"],[1680161790,"896323"],[1680161805,"896449"],[1680161820,"896579"],[1680161835,"896702"],[1680161850,"896832"],[1680161865,"896962"],[1680161880,"897121"],[1680161895,"897267"],[1680161910,"897426"],[1680161925,"897586"],[1680161940,"897750"],[1680161955,"897910"],[1680161970,"898072"],[1680161985,"898210"],[1680162000,"898338"],[1680162015,"898467"],[1680162030,"898599"],[1680162045,"898730"],[1680162060,"898859"],[1680162075,"898988"],[1680162090,"899120"],[1680162105,"899248"],[1680162120,"899374"],[1680162135,"899495"],[1680162150,"899605"],[1680162165,"899725"],[1680162180,"899842"],[1680162195,"899975"],[1680162210,"900108"],[1680162225,"900234"],[1680162240,"900360"],[1680162255,"900484"],[1680162270,"900614"],[1680162285,"900745"],[1680162300,"900874"],[1680162315,"900998"],[1680162330,"901129"],[1680162345,"901260"],[1680162360,"901390"],[1680162375,"901518"],[1680162390,"901645"],[1680162405,"901778"],[1680162420,"901905"],[1680162435,"902031"],[1680162450,"902161"],[1680162465,"902291"],[1680162480,"902420"],[1680162495,"902548"],[1680162510,"902675"],[1680162525,"902802"],[1680162540,"902934"],[1680162555,"903060"],[1680162570,"903189"],[1680162585,"903318"],[1680162600,"903450"],[1680162615,"903573"],[1680162630,"903684"],[1680162645,"903802"],[1680162660,"903921"],[1680162675,"904052"],[1680162690,"904180"],[1680162705,"904314"],[1680162720,"904441"],[1680162735,"904566"],[1680162750,"904698"],[1680162765,"904827"],[1680162780,"904957"],[1680162795,"905082"],[1680162810,"905212"],[1680162825,"905339"],[1680162840,"905468"],[1680162855,"905593"],[1680162870,"905722"],[1680162885,"905853"],[1680162900,"905981"],[1680162915,"906108"],[1680162930,"906234"],[1680162945,"906364"],[1680162960,"906495"],[1680162975,"906623"],[1680162990,"906754"],[1680163005,"906881"],[1680163020,"907010"],[1680163035,"907140"],[1680163050,"907269"],[1680163065,"907398"],[1680163080,"907527"],[1680163095,"907654"],[1680163110,"907782"],[1680163125,"907914"],[1680163140,"908041"],[1680163155,"908169"],[1680163170,"908295"],[1680163185,"908525"],[1680163200,"908658"]]}]"#;

// create local session context with an in-memory table
fn create_context() -> Result<SessionContext> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("__name__", DataType::Utf8, true),
        Field::new("cluster", DataType::Utf8, true),
        Field::new("container", DataType::Utf8, true),
        Field::new("endpoint", DataType::Utf8, true),
        Field::new("exported_endpoint", DataType::Utf8, true),
        Field::new("instance", DataType::Utf8, true),
        Field::new("job", DataType::Utf8, true),
        Field::new("namespace", DataType::Utf8, true),
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));

    let value: Vec<Results> = serde_json::from_str(ORIGIN_VALUE).unwrap();

    let mut field1 = Vec::new();
    let mut field2 = Vec::new();
    let mut field3 = Vec::new();
    let mut field4 = Vec::new();
    let mut field5 = Vec::new();
    let mut field6 = Vec::new();
    let mut field7 = Vec::new();
    let mut field8 = Vec::new();
    let mut field_time = Vec::new();
    let mut field_value = Vec::new();
    for value in value {
        for val in value.values {
            field1.push(value.metric.__name__.clone());
            field2.push(value.metric.cluster.clone());
            field3.push(value.metric.container.clone());
            field4.push(value.metric.endpoint.clone());
            field5.push(value.metric.exported_endpoint.clone());
            field6.push(value.metric.instance.clone());
            field7.push(value.metric.job.clone());
            field8.push(value.metric.namespace.clone());
            field_time.push(val.timestamp * 1000_000);
            field_value.push(val.value.parse::<i64>().unwrap());
        }
    }

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(field1)),
            Arc::new(StringArray::from(field2)),
            Arc::new(StringArray::from(field3)),
            Arc::new(StringArray::from(field4)),
            Arc::new(StringArray::from(field5)),
            Arc::new(StringArray::from(field6)),
            Arc::new(StringArray::from(field7)),
            Arc::new(StringArray::from(field8)),
            Arc::new(Int64Array::from(field_time)),
            Arc::new(Int64Array::from(field_value)),
        ],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(schema, vec![vec![batch1]])?;
    ctx.register_table("zo_http_incoming_requests", Arc::new(provider))?;
    Ok(ctx)
}

#[tokio::main]
async fn main() {
    let start_time = time::Instant::now();
    // let query = r#"rate(zo_http_incoming_requests{cluster="zo1"}[5m])"#;
    let query = r#"topk(1, rate(zo_http_incoming_requests{cluster="zo1"}[5m]))"#;
    let prom_expr = parser::parse(query).unwrap();
    println!("{:?}", prom_expr);
    println!("---------------");

    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH
            .checked_add(Duration::from_secs(1680161100))
            .unwrap(),
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(1680163200))
            .unwrap(),
        interval: Duration::from_secs(15), // step
        lookback_delta: Duration::from_secs(300),
    };

    let ctx = create_context().unwrap();
    println!("prepare time: {}", start_time.elapsed());

    let mut engine = newpromql::QueryEngine::new(ctx);
    let data = engine.exec(eval_stmt).await.unwrap();
    println!("{:?}", data);
    println!("execute time: {}", start_time.elapsed());
}
