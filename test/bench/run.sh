#!/usr/bin/env bash
# Direct driver vs relay, for Postgres and Iceberg. Needs the adbc-iceberg pixi env and docker.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd); root=$(cd "$here/../.." && pwd)
ICEBERG=${ICEBERG:-$HOME/adbc-iceberg}; PY=$ICEBERG/.pixi/envs/default/bin/python
PG_DRIVER=${PG_DRIVER:-$HOME/.config/adbc/drivers/postgresql_linux_amd64_v1.12.0/libadbc_driver_postgresql.so}
RELAY_SO=$root/target/release/libadbc_driver_relay.so; SIDECAR=$root/target/release/relay-sidecar
scratch=$(mktemp -d); P=relay-bench; pids=()
cleanup() { for p in "${pids[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done; docker rm -f $P-pg >/dev/null 2>&1 || true; (cd "$ICEBERG" && docker compose -p $P down -v >/dev/null 2>&1) || true; rm -rf "$scratch"; }
trap cleanup EXIT

echo "== postgres"
docker rm -f $P-pg >/dev/null 2>&1 || true
docker run -d --name $P-pg -p 127.0.0.1:55432:5432 -e POSTGRES_PASSWORD=pw postgres:17 >/dev/null
until docker exec $P-pg pg_isready -q -U postgres -h 127.0.0.1; do sleep 1; done
docker exec -i $P-pg psql -q -U postgres -v ON_ERROR_STOP=1 <<SQL
CREATE TABLE strs AS SELECT i::bigint AS id, 'user' || i AS name, md5(i::text) AS h FROM generate_series(1, 3000000) i;
CREATE TABLE nums AS SELECT i::bigint AS id, i::bigint a, i*2::bigint b, i*3::bigint c, i*4::bigint d, i*0.5::float8 e, i*1.5::float8 f, i*2.5::float8 g, i*3.5::float8 h FROM generate_series(1, 5000000) i;
SQL
URI=postgresql://postgres:pw@127.0.0.1:55432/postgres
cat > "$scratch/pg-sidecar.json" <<JSON
{"socket": "$scratch/pg.sock", "driver": {"name": "$PG_DRIVER", "options": {"uri": "$URI"}}}
JSON
"$SIDECAR" "$scratch/pg-sidecar.json" 2>"$scratch/pg.log" & pids+=($!)
until [ -S "$scratch/pg.sock" ]; do sleep 0.1; done
cat > "$scratch/pg.json" <<JSON
{"driver": "$PG_DRIVER", "options": {"uri": "$URI"}, "relay_driver": "$RELAY_SO", "socket": "$scratch/pg.sock",
 "queries": [["strings", "SELECT id, name, h FROM public.strs"], ["wide numeric", "SELECT id, a, b, c, d, e, f, g, h FROM public.nums"]]}
JSON
"$PY" "$here/compare.py" "$scratch/pg.json"

echo "== iceberg (parquet on minio)"
(cd "$ICEBERG" && docker compose -p $P up -d >/dev/null 2>&1)
until curl -sf http://localhost:8181/v1/config >/dev/null; do sleep 1; done
sleep 3
"$PY" "$here/seed_iceberg.py"
IOPTS='{"uri": "http://localhost:8181", "adbc.iceberg.catalog.name": "rest", "adbc.iceberg.s3.endpoint": "http://localhost:9000", "adbc.iceberg.s3.region": "us-east-1", "adbc.iceberg.s3.access_key": "admin", "adbc.iceberg.s3.secret_key": "password"}'
ISO=$ICEBERG/build/libadbc_driver_iceberg_rust.so
cat > "$scratch/ib-sidecar.json" <<JSON
{"socket": "$scratch/ib.sock", "driver": {"name": "$ISO", "entrypoint": "AdbcDriverIcebergInit", "options": $IOPTS}}
JSON
"$SIDECAR" "$scratch/ib-sidecar.json" 2>"$scratch/ib.log" & pids+=($!)
until [ -S "$scratch/ib.sock" ]; do sleep 0.1; done
cat > "$scratch/ib.json" <<JSON
{"driver": "$ISO", "entrypoint": "AdbcDriverIcebergInit", "options": $IOPTS, "relay_driver": "$RELAY_SO", "socket": "$scratch/ib.sock",
 "queries": [["allowed columns", "SELECT id, name, value FROM default.relay_employees"]]}
JSON
"$PY" "$here/compare.py" "$scratch/ib.json"
"$PY" - "$RELAY_SO" "$scratch/ib.sock" "$ISO" "$IOPTS" <<'PY'
import json, sys
import adbc_driver_manager.dbapi as dbapi
so, sock, iso, opts = sys.argv[1:]
def q(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql); return cur.fetch_arrow_table()
d = dbapi.connect(driver=iso, entrypoint="AdbcDriverIcebergInit", db_kwargs=json.loads(opts), autocommit=True)
raw = q(d, "SELECT * FROM default.relay_employees WHERE id = 7")
print("direct driver sees:", raw.to_pylist()[0], "field metadata:", raw.schema.field("id").metadata)
r = dbapi.connect(driver=so, entrypoint="AdbcDriverRelayInit", db_kwargs={"uri": sock}, autocommit=True)
t = q(r, "SELECT * FROM default.relay_employees WHERE id = 7")
print("relay sees:        ", t.to_pylist()[0], "field metadata:", t.schema.field("id").metadata, "schema metadata:", t.schema.metadata)
assert t.to_pylist() == raw.to_pylist()
PY
cat "$scratch/ib.log" | grep -iE 'error|violation' || true
