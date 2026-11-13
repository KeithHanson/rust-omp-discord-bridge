# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.99.1
FROM rust:${RUST_VERSION}-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bin omp-discord-bridge

FROM debian:bookworm-slim AS omp-installer

ARG OMP_VERSION=18.20.0
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl --fail --show-error --silent --location \
        --connect-timeout 10 --max-time 60 \
        "https://raw.githubusercontent.com/can1357/oh-my-pi/v${OMP_VERSION}/scripts/install.sh" \
        --output /tmp/install-omp.sh \
    && PI_INSTALL_DIR=/opt/omp sh /tmp/install-omp.sh --binary --ref "v${OMP_VERSION}" \
    && /opt/omp/omp --version \
    && rm /tmp/install-omp.sh

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates \
        git \
        libgcc-s1 \
        libstdc++6 \
        openssh-client \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 bridge \
    && useradd --uid 1000 --gid bridge --home-dir /data --no-create-home --shell /usr/sbin/nologin bridge \
    && mkdir -p /config /data /workspace \
    && chown -R bridge:bridge /data /workspace

COPY --from=builder /build/target/release/omp-discord-bridge /usr/local/bin/omp-discord-bridge
COPY --from=omp-installer /opt/omp/omp /usr/local/bin/omp

ENV HOME=/data \
    OMP_DISCORD_BRIDGE_CONFIG=/config/config.toml \
    RUST_LOG=info

WORKDIR /workspace
USER bridge:bridge

ENTRYPOINT ["omp-discord-bridge"]
CMD ["run"]
