# Prometheus parse test

Test implement PromQL parse with Datafusion.

## update samples

```shell
./update-samples.sh
```

## sample query

```
histogram_quantile(0.9, sum by (le, exported_endpoint) (rate(zo_http_response_time_bucket{namespace="ziox-alpha1",organization="default",exported_endpoint=~".*_json"}[5m])))
```

## sample data URLs

```
zo_http_incoming_requests{namespace="ziox-alpha1",organization="default"}[35m]
```

http://localhost:9090/api/v1/query?query=zo_http_incoming_requests%7Bnamespace%3D%22ziox-alpha1%22%2Corganization%3D%22default%22%7D%5B35m%5D&time=1682496000.385


```
zo_storage_files{namespace="ziox-alpha1",organization="default"}[35m]
```

http://localhost:9090/api/v1/query?query=zo_storage_files%7Bnamespace%3D%22ziox-alpha1%22%2Corganization%3D%22default%22%7D%5B35m%5D&time=1682496000.385

```
zo_http_response_time_bucket{namespace="ziox-alpha1",organization="default"}[35m]
```

http://localhost:9090/api/v1/query?query=zo_http_response_time_bucket%7Bnamespace%3D%22ziox-alpha1%22%2Corganization%3D%22default%22%7D%5B35m%5D&time=1682496000.385

http://localhost:9090/api/v1/query?query=zo_http_response_time_count%7Bnamespace%3D%22ziox-alpha1%22%2Corganization%3D%22default%22%7D%5B35m%5D&time=1682496000.385

http://localhost:9090/api/v1/query?query=zo_http_response_time_sum%7Bnamespace%3D%22ziox-alpha1%22%2Corganization%3D%22default%22%7D%5B35m%5D&time=1682496000.385
