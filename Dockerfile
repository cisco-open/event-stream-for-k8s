FROM docker.io/library/rust:1.85 AS builder

WORKDIR /usr/src/app
COPY . .
RUN cargo install --path .

FROM gcr.io/distroless/cc

WORKDIR /
COPY --from=builder /usr/local/cargo/bin/event-stream-for-k8s /
CMD ["/event-stream-for-k8s"]
