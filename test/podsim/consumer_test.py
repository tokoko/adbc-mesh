"""Runs in the untrusted container: normal use first, then attacks. Exits non-zero on any failure."""
import array, fcntl, glob, json, mmap, os, socket, sys
import adbc_driver_manager.dbapi as dbapi
import pyarrow as pa

SOCK = os.environ.get("RELAY_SOCK", "/run/adbc-relay/relay.sock")
LOWCAP_SOCK = os.environ.get("RELAY_LOWCAP_SOCK")
DRIVER = "/opt/relay/libadbc_driver_relay.so"
HIDDEN = (b"SECRET-SSN", b"SECRET-TOKEN", b"s3cr3t-pg-password")
failures = 0

def check(name, ok, detail=""):
    global failures
    failures += 0 if ok else 1
    print(f"{'PASS' if ok else 'FAIL'}  {name}{'  ' + str(detail) if detail else ''}", flush=True)

def connect(**extra):
    return dbapi.connect(driver=DRIVER, entrypoint="AdbcDriverRelayInit", db_kwargs={"uri": SOCK, **extra}, autocommit=True)

def query(sql):
    with connect() as c, c.cursor() as cur:
        cur.execute(sql)
        return cur.fetch_arrow_table()

def refused(name, sql, want):
    try:
        t = query(sql)
        check(name, False, f"returned {t.num_rows} rows: {t.schema.names}")
    except Exception as e:
        check(name, want in str(e), str(e).splitlines()[0][:90])

print(f"running as uid={os.getuid()}\n\n-- normal reads")
t = query("SELECT id, name, dept, active, score, hired FROM public.employees")
check("granted columns arrive", t.schema.names == ["id", "name", "dept", "active", "score", "hired"], t.schema.names)
check("all rows arrive", t.num_rows == 2000000, t.num_rows)
t = t.sort_by("id")
row = t.slice(1, 1).to_pylist()[0]
check("values, nulls and types survive", row["id"] == 2 and row["name"] == "user2" and row["dept"] == "eng"
      and row["active"] is False and row["score"] == 1.0 and row["hired"].minute == 2, row)
check("nulls are preserved", t.slice(0, 1).to_pylist()[0]["dept"] is None and t["dept"].null_count == 1000000)
t.validate(full=True)
check("arrow full validation", True)
f = query("SELECT id, name FROM public.employees WHERE id <= 10 AND NOT (dept IS NULL OR id = 4)")
check("filters on allowed columns work", sorted(f["id"].to_pylist()) == [2, 6, 8, 10], f["id"].to_pylist())
check("empty results work", query("SELECT id FROM public.employees WHERE id < 0").num_rows == 0)

print("\n-- zero copy, as seen from the consumer's address space")
# Read fresh: sort_by above copied `t` out of the mappings.
raw = query("SELECT id, name, dept FROM public.employees")
regions = []
for line in open("/proc/self/maps"):
    if "memfd:adbc-relay" in line:
        lo, hi = (int(x, 16) for x in line.split()[0].split("-"))
        regions.append((lo, hi, line.split()[1]))
addrs = [b.address for col in raw.columns for chunk in col.chunks for b in chunk.buffers() if b is not None and b.size]
inside = sum(any(lo <= a < hi for lo, hi, _ in regions) for a in addrs)
check("every arrow buffer lies inside a memfd mapping", addrs and inside == len(addrs), f"{inside}/{len(addrs)} buffers")
check("every mapping is read-only", all(p.startswith("r--") for _, _, p in regions), {p for _, _, p in regions})
del raw, t, f

print("\n-- SQL is forwarded untouched")
check("aggregates", query("SELECT count(*) AS n FROM public.employees").to_pylist()[0]["n"] == 2000000)
check("functions and aliases", query("SELECT upper(name) AS u FROM public.employees WHERE id = 1").to_pylist() == [{"u": "USER1"}])
check("runs as the sidecar's role", query("SELECT current_user AS u").to_pylist() == [{"u": "relay_reader"}])

print("\n-- the backend role's grants are what stop these")
for name, sql in [
    ("hidden column", "SELECT salary FROM public.employees"),
    ("star includes hidden columns", "SELECT * FROM public.employees"),
    ("alias laundering", "SELECT salary AS id FROM public.employees"),
    ("function", "SELECT upper(ssn) FROM public.employees"),
    ("predicate oracle", "SELECT id FROM public.employees WHERE salary > 100"),
    ("order by oracle", "SELECT id FROM public.employees ORDER BY salary"),
    ("union", "SELECT id FROM public.employees UNION SELECT salary FROM public.employees"),
    ("subquery in FROM", "SELECT id FROM (SELECT salary AS id FROM public.employees) t"),
    ("subquery in WHERE", "SELECT id FROM public.employees WHERE id = (SELECT salary FROM public.employees LIMIT 1)"),
    ("stacked statement", "SELECT id FROM public.employees WHERE id = 1; SELECT ssn FROM public.employees"),
    ("hidden table", "SELECT token FROM public.secrets"),
    ("server file read", "SELECT pg_read_file('/etc/passwd')"),
    ("write", "DELETE FROM public.employees"),
]:
    refused(name, sql, "backend error")

print("\n-- metadata")
with connect() as c:
    s = c.adbc_get_table_schema("employees", db_schema_filter="public")
    print(f"      note: table schema shows all columns, hidden ones included: {s.names}")
    check("table schema is forwarded", "id" in s.names)
    try:
        c.adbc_get_objects(depth="all").read_all()
        check("catalog listing refused", False)
    except Exception as e:
        check("catalog listing refused", "not supported" in str(e), str(e)[:70])

print("\n-- backend retargeting through options")
for key, value in [("adbc.iceberg.s3.endpoint", "http://evil:9000"), ("password", "x"), ("username", "postgres")]:
    try:
        connect(**{key: value}).close()
        check(f"option {key}", False)
    except Exception as e:
        check(f"option {key}", "refused" in str(e), str(e)[:80])

print("\n-- raw protocol: tampering with a batch we were handed")
def raw():
    s = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    s.connect(SOCK)
    return s
def recv(s):
    msg, fds, _, _ = socket.recv_fds(s, 131072, 4)
    return json.loads(msg) if msg else None, fds
def send(s, m):
    s.send(json.dumps(m).encode())

s = raw()
send(s, {"Query": {"sql": "SELECT id, name FROM public.employees WHERE id <= 1000"}})
recv(s)
send(s, "Next")
msg, fds = recv(s)
fd = fds[0]
size = msg["Batch"]["len"]
F_GET_SEALS = 1034; WANT = 0x1 | 0x2 | 0x4 | 0x8
check("batch memfd has all four seals", fcntl.fcntl(fd, F_GET_SEALS) & WANT == WANT, hex(fcntl.fcntl(fd, F_GET_SEALS)))
def blocked(name, f):
    try:
        f()
        check(name, False)
    except OSError as e:
        check(name, True, e.strerror)
blocked("mmap(PROT_WRITE, MAP_SHARED)", lambda: mmap.mmap(fd, size, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE))
blocked("write()", lambda: os.write(fd, b"x"))
blocked("ftruncate(0)", lambda: os.ftruncate(fd, 0))
try:
    rw = os.open(f"/proc/self/fd/{fd}", os.O_RDWR)
    print(f"      note: /proc reopen as O_RDWR succeeded, so the seals below are all that protect the batch")
    blocked("write() via reopened fd", lambda: os.write(rw, b"x"))
    blocked("ftruncate() via reopened fd", lambda: os.ftruncate(rw, 0))
    blocked("mmap(PROT_WRITE) via reopened fd", lambda: mmap.mmap(rw, size, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE))
    blocked("adding seals via reopened fd", lambda: fcntl.fcntl(rw, 1033, 0x10))
except OSError as e:
    check("/proc reopen as O_RDWR refused", True, e.strerror)
m = mmap.mmap(fd, size, mmap.MAP_SHARED, mmap.PROT_READ)
page = m[:]
check("batch contains only what was asked for", not any(h in page for h in HIDDEN) and b"user1" in page)
# A private mapping is allowed but is only ever our own copy.
p = mmap.mmap(fd, size, mmap.MAP_PRIVATE, mmap.PROT_READ | mmap.PROT_WRITE); p[0] = p[0] ^ 0xff
check("private write does not reach the shared pages", m[0] != p[0])
s.close()

print("\n-- raw protocol: abuse")
s = raw(); s.send(b"{not json"); recv(s); s.close(); check("garbage frame survived", True)
s = raw(); send(s, "Next"); recv(s); s.close(); check("Next before Query survived", True)
r, w = os.pipe()
held = []
for _ in range(50):
    s = raw(); socket.send_fds(s, [json.dumps({"Query": {"sql": "SELECT id FROM public.employees WHERE id < 0"}}).encode()], [r, w]); recv(s); s.close()
check("fd-laden requests survived", True)
s = raw(); send(s, {"Query": {"sql": "SELECT id FROM public.employees WHERE " + "(" * 20000 + "id=1" + ")" * 20000}}); m2, _ = recv(s); s.close()
check("deep nesting survived", True, m2 and str(m2)[:70])
s = raw(); s.send(b"x" * 100000); recv(s); s.close(); check("oversized garbage survived", True)
# Never release anything: the sidecar has to stop handing out memory.
s = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET); s.connect(LOWCAP_SOCK or SOCK)
send(s, {"Query": {"sql": "SELECT id, name, dept, active, score, hired FROM public.employees"}}); recv(s)
got = 0; err = None
for _ in range(100000):
    send(s, "Next")
    msg, fds = recv(s)
    for x in fds: os.close(x)
    if "Batch" in (msg or {}): got += msg["Batch"]["len"]
    else: err = msg; break
if LOWCAP_SOCK:
    check("holding batches hits the cap", isinstance(err, dict) and err.get("Error", {}).get("kind") == "Exhausted", f"after {got/2**20:.0f} MiB: {err}")
s.close()
check("sidecar still serves after all of that", query("SELECT id FROM public.employees WHERE id = 1").num_rows == 1)

print("\n-- what this container can see of the backend")
leaks = []
for path in glob.glob("/proc/[0-9]*/environ") + glob.glob("/proc/[0-9]*/cmdline"):
    try:
        data = open(path, "rb").read()
        if any(h in data for h in HIDDEN): leaks.append(path)
    except OSError:
        pass
check("no credential in any visible process's env or cmdline", not leaks, leaks)
check("no sidecar process visible", not any("relay-sidecar" in open(p, "rb").read().decode(errors="ignore") for p in glob.glob("/proc/[0-9]*/cmdline") if os.access(p, os.R_OK)))
check("sidecar config not mounted here", not os.path.exists("/etc/adbc-relay"))
if os.environ.get("EXPECT_ISOLATED"):
    print("\n-- pod isolation")
    check("no service account token", not os.path.exists("/var/run/secrets/kubernetes.io"))
    check("runs as a non-root uid", os.getuid() != 0, os.getuid())
# There's no egress control, so this is expected. The backend has to authenticate every request.
try:
    socket.create_connection((os.environ.get("BACKEND_HOST", "pg"), 5432), timeout=3).close()
    print("      note: the database is reachable from here; only its authentication stops direct access")
except OSError as e:
    print(f"      note: database not reachable from here: {e}")
print(f"\n{failures} failure(s)")
sys.exit(1 if failures else 0)
