FROM public.ecr.aws/zinclabs/rust:bullseye-sccache as builder
RUN rustc --version && sccache --version
WORKDIR /app
COPY . /app
RUN cargo build --release

FROM public.ecr.aws/debian/debian:bullseye-slim as runtime
RUN set -eux; \
    apt-get -q update; \
    DEBIAN_FRONTEND=noninteractive apt-get -qy install --no-install-recommends \
        ca-certificates; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/prom-test /app/
COPY --from=builder /app/update-samples.sh /app/update-samples.sh
COPY --from=builder /app/samples /app/samples
CMD ["./prom-test", "--server"]
