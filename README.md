# PromQL engine experiments

Test implementation of PromQL processor based on DataFusion.

## Run HTTP API server

```shell
cargo run --release -- --server
```

## Run a single PromQL query

```shell
cargo run --release -- --debug 'zo_http_incoming_requests{namespace="ziox-alpha1",organization="default"}'
```

## Fetch new metrics data

```shell
./update-samples.sh
```

### in k8s update for test

```shell
apt update
apt install -y curl
sed -i 's#localhost#prometheus#g' update-samples.sh
./update-samples.sh
```

## Build Docker image

- Install [`just`](https://github.com/casey/just) tool.
- Run `just docker-build`.

## PromQL query examples

```promql
zo_http_incoming_requests{namespace="ziox-alpha1",organization="default"}
irate(zo_http_response_time_count{namespace="ziox-alpha1",organization="default"}[5m])
topk(1, irate(zo_http_response_time_count{namespace="ziox-alpha1",organization="default"}[5m]))
histogram_quantile(0.9, rate(zo_http_response_time_bucket{namespace="ziox-alpha1",organization="default"}[5m]))
topk(2,histogram_quantile(0.8, rate(zo_http_response_time_bucket{namespace="ziox-alpha1",organization="default"}[5m])))
histogram_quantile(0.9, sum by (le, exported_endpoint) (rate(zo_http_response_time_bucket{namespace="ziox-alpha1",organization="default",exported_endpoint=~".*_json"}[5m])))
```

## Sample data URLs

```promql
zo_http_incoming_requests{namespace="ziox-alpha1",organization="default"}[35m]
```

<http://localhost:9090/api/v1/query?query=zo_http_incoming_requests%7Bnamespace%3D%22ziox-alpha1%22%2Corganization%3D%22default%22%7D%5B35m%5D&time=1682496000.385>

```promql
zo_storage_files{namespace="ziox-alpha1",organization="default"}[35m]
```

<http://localhost:9090/api/v1/query?query=zo_storage_files%7Bnamespace%3D%22ziox-alpha1%22%2Corganization%3D%22default%22%7D%5B35m%5D&time=1682496000.385>

```promql
zo_http_response_time_bucket{namespace="ziox-alpha1",organization="default"}[35m]
```

<http://localhost:9090/api/v1/query?query=zo_http_response_time_bucket%7Bnamespace%3D%22ziox-alpha1%22%2Corganization%3D%22default%22%7D%5B35m%5D&time=1682496000.385>

<http://localhost:9090/api/v1/query?query=zo_http_response_time_count%7Bnamespace%3D%22ziox-alpha1%22%2Corganization%3D%22default%22%7D%5B35m%5D&time=1682496000.385>

<http://localhost:9090/api/v1/query?query=zo_http_response_time_sum%7Bnamespace%3D%22ziox-alpha1%22%2Corganization%3D%22default%22%7D%5B35m%5D&time=1682496000.385>

## Benchmarking

1. Run benchmarks

   ```shell
   cargo bench
   ```

2. Install [`flamegraph`](https://github.com/flamegraph-rs/flamegraph) cargo subcommand

   ```shell
   cargo install flamegraph
   ```

3. Generate `flamegraph.svg`

   ```shell
   # NOTE: the last `--bench` is required for `criterion` to run in benchmark mode, instead of test mode
   CARGO_PROFILE_BENCH_DEBUG=true cargo flamegraph --root --bench it -- --bench
   ```

4. Open `flamegraph.svg` in your browser.
