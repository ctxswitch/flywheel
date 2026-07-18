# syntax=docker/dockerfile:1

FROM rust:1.94-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends clang cmake libclang-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/flywheel

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && install -d -o 65532 -g 65532 /var/lib/flywheel

COPY --from=builder /usr/src/flywheel/target/release/flywheel /usr/local/bin/flywheel

USER 65532:65532
WORKDIR /var/lib/flywheel

ENTRYPOINT ["flywheel"]
CMD ["serve"]
