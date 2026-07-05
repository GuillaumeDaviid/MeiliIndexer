# syntax=docker/dockerfile:1.7

FROM rust:1.96-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN --mount=type=cache,target=/app/target \
    --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --locked && \
    cp /app/target/release/meili-mysql-sync /usr/local/bin/meili-mysql-sync

RUN mkdir -p /image-root/config /image-root/data && \
    touch /image-root/data/.keep

FROM gcr.io/distroless/cc-debian12:nonroot

WORKDIR /data

ENV RUST_LOG=info

COPY --from=builder /usr/local/bin/meili-mysql-sync /usr/local/bin/meili-mysql-sync
COPY --from=builder /lib/x86_64-linux-gnu/libz.so.1 /lib/x86_64-linux-gnu/libz.so.1
COPY --from=builder --chown=nonroot:nonroot /image-root/data /data
COPY --chown=nonroot:nonroot config.example.toml /config/config.example.toml

VOLUME ["/config", "/data"]

# Mount the real config at /config/config.toml. The state file stays in /data
# when runtime.state_path is relative, as in config.example.toml.
ENTRYPOINT ["/usr/local/bin/meili-mysql-sync", "--config", "/config/config.toml"]
CMD ["run"]
