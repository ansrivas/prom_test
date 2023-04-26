#!/usr/bin/env bash
set -eu -o pipefail

timestamp=$(date +%s)

echo "start time: ${timestamp}"
echo "${timestamp}" > $(git rev-parse --show-toplevel)/samples/timestamp.log 

curl -o ./samples/counter.json "http://localhost:9090/api/v1/query?query=zo_http_incoming_requests%7Bnamespace%3D%22ziox-alpha1%22%2Corganization%3D%22default%22%7D%5B35m%5D&time=${timestamp}.385"
curl -o ./samples/gauge.json "http://localhost:9090/api/v1/query?query=zo_storage_files%7Bnamespace%3D%22ziox-alpha1%22%2Corganization%3D%22default%22%7D%5B35m%5D&time=${timestamp}.385"
curl -o ./samples/histogram_bucket.json "http://localhost:9090/api/v1/query?query=zo_http_response_time_bucket%7Bnamespace%3D%22ziox-alpha1%22%2Corganization%3D%22default%22%7D%5B35m%5D&time=${timestamp}.385"
curl -o ./samples/histogram_count.json "http://localhost:9090/api/v1/query?query=zo_http_response_time_count%7Bnamespace%3D%22ziox-alpha1%22%2Corganization%3D%22default%22%7D%5B35m%5D&time=${timestamp}.385"
curl -o ./samples/histogram_sum.json "http://localhost:9090/api/v1/query?query=zo_http_response_time_sum%7Bnamespace%3D%22ziox-alpha1%22%2Corganization%3D%22default%22%7D%5B35m%5D&time=${timestamp}.385"

echo "done"
