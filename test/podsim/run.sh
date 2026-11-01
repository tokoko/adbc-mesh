#!/usr/bin/env bash
# Simulates the injected pod with plain containers: a sidecar and a consumer with different uids,
# sharing a network namespace and a socket volume, with separate PID namespaces and mounts.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd); root=$(cd "$here/../.." && pwd)
PG_DRIVER=${PG_DRIVER:-$HOME/.config/adbc/drivers/postgresql_linux_amd64_v1.12.0/libadbc_driver_postgresql.so}
scratch=$(mktemp -d); N=relay-podsim
cleanup() { docker rm -f $N-consumer $N-sidecar $N-sidecar-lowcap $N-pause $N-pg >/dev/null 2>&1 || true; docker network rm $N >/dev/null 2>&1 || true; docker volume rm $N-sock >/dev/null 2>&1 || true; rm -rf "$scratch"; }
trap cleanup EXIT; cleanup
docker build -q -t $N "$here" >/dev/null 2>&1
docker network create $N >/dev/null
docker volume create $N-sock >/dev/null
docker run -d --name $N-pg --network $N --network-alias pg -e POSTGRES_PASSWORD=s3cr3t-pg-password postgres:17 >/dev/null
until docker exec $N-pg pg_isready -q -U postgres -h 127.0.0.1; do sleep 1; done
docker exec -i $N-pg psql -q -U postgres -v ON_ERROR_STOP=1 < "$here/seed.sql"

mkdir -p "$scratch/config" "$scratch/secret"
cat > "$scratch/config/config.json" <<JSON
{"socket": "/run/adbc-relay/relay.sock",
 "driver": {"name": "/opt/relay/libadbc_driver_postgresql.so", "options_from_files": {"uri": "/etc/adbc-relay/secret/uri"}},
 "max_outstanding_bytes": 268435456}
JSON
printf 'postgresql://relay_reader:s3cr3t-pg-password@pg:5432/postgres' > "$scratch/secret/uri"
chmod -R a+rX "$scratch"; cp "$PG_DRIVER" "$scratch/"

docker run -d --name $N-pause --network $N python:3.12-slim sleep infinity >/dev/null
docker run --rm -v $N-sock:/run/adbc-relay python:3.12-slim chmod 1777 /run/adbc-relay
docker run -d --name $N-sidecar --network container:$N-pause --user 1337:1337 --read-only --cap-drop ALL \
  -v $N-sock:/run/adbc-relay \
  -v "$scratch/config/config.json:/etc/adbc-relay/config.json:ro" \
  -v "$scratch/secret:/etc/adbc-relay/secret:ro" \
  -v "$root/target/release/relay-sidecar:/opt/relay/relay-sidecar:ro" \
  -v "$scratch/libadbc_driver_postgresql.so:/opt/relay/libadbc_driver_postgresql.so:ro" \
  $N /opt/relay/relay-sidecar /etc/adbc-relay/config.json >/dev/null
# Same backend with a small cap on unreleased data, for the test that never releases anything.
sed -e 's/relay.sock/lowcap.sock/' -e 's/268435456/16777216/' "$scratch/config/config.json" > "$scratch/config/lowcap.json"; chmod a+r "$scratch/config/lowcap.json"
docker run -d --name $N-sidecar-lowcap --network container:$N-pause --user 1337:1337 --read-only --cap-drop ALL \
  -v $N-sock:/run/adbc-relay \
  -v "$scratch/config/lowcap.json:/etc/adbc-relay/config.json:ro" \
  -v "$scratch/secret:/etc/adbc-relay/secret:ro" \
  -v "$root/target/release/relay-sidecar:/opt/relay/relay-sidecar:ro" \
  -v "$scratch/libadbc_driver_postgresql.so:/opt/relay/libadbc_driver_postgresql.so:ro" \
  $N /opt/relay/relay-sidecar /etc/adbc-relay/config.json >/dev/null
for _ in $(seq 30); do docker logs $N-sidecar 2>&1 | grep -q listening && break; sleep 0.5; done
docker logs $N-sidecar 2>&1 | head -3

status=0
docker run --name $N-consumer --network container:$N-pause --user 1000:1000 --cap-drop ALL \
  -v $N-sock:/run/adbc-relay \
  -v "$root/target/release/libadbc_driver_relay.so:/opt/relay/libadbc_driver_relay.so:ro" \
  -v "$here/consumer_test.py:/test/consumer_test.py:ro" \
  -e RELAY_LOWCAP_SOCK=/run/adbc-relay/lowcap.sock \
  $N python /test/consumer_test.py || status=$?
echo; echo "-- sidecar log (tail)"; docker logs $N-sidecar 2>&1 | grep -v '^connection from' | tail -25
docker inspect $N-sidecar --format 'sidecar running={{.State.Running}} restarts={{.RestartCount}} oom={{.State.OOMKilled}}'
exit $status
