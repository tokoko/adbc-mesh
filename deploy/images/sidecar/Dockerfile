FROM debian:bookworm-slim
COPY relay-sidecar libadbc_driver_postgresql.so /opt/relay/
ENTRYPOINT ["/opt/relay/relay-sidecar"]
