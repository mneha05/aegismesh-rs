FROM rust:slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN useradd --create-home --uid 10001 aegis
COPY --from=build /src/target/release/aegis-node /usr/local/bin/aegis-node
COPY --from=build /src/target/release/aegis-gateway /usr/local/bin/aegis-gateway
USER aegis
ENV BIND=0.0.0.0:8080
CMD ["aegis-node"]
