FROM --platform=linux/amd64 artifactory.thousandeyes.com/docker/te-rust-musl-builder:latest AS builder

ARG target=x86_64-unknown-linux-gnu

WORKDIR /home/rust/src

COPY . .

RUN --mount=type=cache,target=/home/rust/.cargo/git \
    --mount=type=cache,target=/home/rust/.cargo/registry \
    --mount=type=cache,target=target \
    cargo install --path=. --target=${target}

FROM artifactory.thousandeyes.com/docker-hub/ubuntu:latest

WORKDIR /

COPY --from=builder /etc/ssl /etc/ssl
COPY --from=builder /home/rust/.cargo/bin/kubernetes-event-stream /

CMD []
ENTRYPOINT [ "/kubernetes-event-stream"]
