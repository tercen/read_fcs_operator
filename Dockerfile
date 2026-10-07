# syntax=docker/dockerfile:1.7
# read_fcs_operator — static tier (create-rust-operator skill §5): one musl binary on scratch.

# ---- builder ----
FROM rust:1.94-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
        musl-tools protobuf-compiler pkg-config git ca-certificates \
 && rm -rf /var/lib/apt/lists/* && rustup target add x86_64-unknown-linux-musl
WORKDIR /build
RUN cargo install cargo-chef --locked
COPY Cargo.toml Cargo.lock ./
RUN cargo chef prepare --recipe-path recipe.json
RUN cargo chef cook --release --target x86_64-unknown-linux-musl --recipe-path recipe.json
COPY src ./src
COPY operator.json ./
RUN cargo build --release --target x86_64-unknown-linux-musl --bin read_fcs_operator \
 && ls -l target/x86_64-unknown-linux-musl/release/read_fcs_operator
# writable temp dir for uid 1000 (downloads, extracted FCS files, result TSON)
RUN mkdir -p /tmp-op && chown 1000:1000 /tmp-op

# ---- runtime ----
FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/target/x86_64-unknown-linux-musl/release/read_fcs_operator /usr/local/bin/read_fcs_operator
COPY --from=builder --chown=1000:1000 /tmp-op /tmp
COPY operator.json /operator/operator.json
USER 1000:1000
WORKDIR /operator
ENV RUST_BACKTRACE=1 RUST_LOG=info TMPDIR=/tmp
ENTRYPOINT ["/usr/local/bin/read_fcs_operator"]
