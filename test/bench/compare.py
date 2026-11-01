"""Compare a direct ADBC driver with the same driver behind the relay. Usage: compare.py <config.json>"""
import json, statistics, sys, time
import adbc_driver_manager.dbapi as dbapi
import pyarrow.compute as pc

cfg = json.load(open(sys.argv[1]))
def direct():
    return dbapi.connect(driver=cfg["driver"], entrypoint=cfg.get("entrypoint"), db_kwargs=cfg["options"], autocommit=True)
def relay():
    return dbapi.connect(driver=cfg["relay_driver"], entrypoint="AdbcDriverRelayInit", db_kwargs={"uri": cfg["socket"]}, autocommit=True)

def once(open_conn, sql):
    with open_conn() as c, c.cursor() as cur:
        start = time.perf_counter()
        cur.execute(sql)
        t = cur.fetch_arrow_table()
        for name in t.schema.names:      # touch the data so lazily mapped pages get charged
            pc.count(t[name])
        pc.sum(t[t.schema.names[0]])
        return time.perf_counter() - start, t.nbytes, t.num_rows, t["id"].num_chunks if "id" in t.schema.names else 0

for label, sql in cfg["queries"]:
    res = {}
    for name, fn in (("direct", direct), ("relay", relay)):
        once(fn, sql)  # warm up
        runs = [once(fn, sql) for _ in range(cfg.get("runs", 7))]
        res[name] = statistics.median(r[0] for r in runs); nbytes, rows, batches = runs[0][1:]
    gb = nbytes / 1e9
    overhead = (res["relay"] / res["direct"] - 1) * 100
    print(f"{label:24} {rows:>9} rows {batches:>5} batches {gb:6.2f} GB  direct {res['direct']*1e3:8.1f} ms  relay {res['relay']*1e3:8.1f} ms  {overhead:+6.1f}%", flush=True)
