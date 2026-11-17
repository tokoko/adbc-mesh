"""A Spark job reading Postgres through the relay. Runs as the driver of a real spark-submit."""
import json, os, re, ssl, sys, urllib.error, urllib.request
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOCK = "/run/adbc-mesh/postgres.sock"
TOKEN = "/var/run/secrets/kubernetes.io/serviceaccount/token"
CA = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
NS = "relay-spark"
COLS = "id, name, dept, active, score, hired"
failures = 0

def check(name, ok, detail=""):
    global failures
    failures += 0 if ok else 1
    print(f"{'PASS' if ok else 'FAIL'}  {name}{'  ' + str(detail)[:150] if detail != '' else ''}", flush=True)

spark = SparkSession.builder.appName("relay-test").getOrCreate()

def reader(**extra):
    r = (spark.read.format("com.tokoko.spark.adbc")
         .option("driver", "org.apache.arrow.adbc.driver.jni.JniDriverFactory")
         .option("jni.driver", "/opt/relay/libadbc_driver_relay.so")
         .option("dialect", "postgresql")
         .option("uri", SOCK))
    for k, v in extra.items():
        r = r.option(k, v)
    return r

print("-- reading through the relay")
query = f"SELECT {COLS} FROM public.employees"
df = reader(query=query).load()
check("schema has the granted columns", df.columns == COLS.split(", "), df.columns)
rows = df.agg(F.count("*").alias("n"), F.sum("id").alias("s"), F.count("dept").alias("d")).collect()[0]
check("all rows, with nulls preserved", (rows.n, rows.s, rows.d) == (2000000, 2000001000000, 1000000), rows)
one = reader(query=query).load().filter("id = 2").collect()
check("filters and values", len(one) == 1 and one[0].name == "user2" and one[0].dept == "eng", one)
parts = reader(dbtable="(SELECT id, name, dept FROM public.employees) t", partitionColumn="id",
               lowerBound="1", upperBound="2000001", numPartitions="6").load()
check("partitioned read across tasks", parts.rdd.getNumPartitions() == 6 and parts.count() == 2000000, parts.rdd.getNumPartitions())
try:
    reader(query="SELECT ssn FROM public.employees").load().collect()
    check("hidden column refused", False)
except Exception as e:
    check("hidden column refused", "backend error" in str(e), str(e).splitlines()[0])

print("-- what executors can see")
def probe(_):
    host = os.uname().nodename
    yield json.dumps({
        "host": host, "uid": os.getuid(), "token": os.path.exists(TOKEN),
        "socket": os.path.exists(SOCK),
        "secret": os.path.exists("/etc/adbc-mesh/uri") or os.path.exists("/var/run/secrets/adbc-mesh/token"),
    })
seen = {json.loads(x)["host"]: json.loads(x) for x in spark.sparkContext.parallelize(range(32), 32).mapPartitions(probe).collect()}
execs = [v for v in seen.values()]
check("ran on more than one executor", len(execs) >= 2, sorted(seen))
check("executors have the socket", all(v["socket"] for v in execs))
check("executors have no API token", not any(v["token"] for v in execs))
check("executors have no credentials", not any(v["secret"] for v in execs))

print("-- what the driver can do with its token")
check("driver has an API token", os.path.exists(TOKEN))
check("driver has no credentials", not os.path.exists("/etc/adbc-mesh/uri") and not os.path.exists("/var/run/secrets/adbc-mesh/token"))
ctx = ssl.create_default_context(cafile=CA)
def api(method, path, body=None):
    req = urllib.request.Request(
        "https://kubernetes.default.svc" + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": "Bearer " + open(TOKEN).read(), "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=20) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
def pod(name, spec, labels=None):
    return {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "labels": labels or {}},
            "spec": {"containers": [{"name": "app", "image": "evil"}], **spec}}
def dry_create(body):
    return api("POST", f"/api/v1/namespaces/{NS}/pods?dryRun=All", body)
code, _ = api("GET", f"/api/v1/namespaces/{NS}/pods")
check("can list pods", code == 200, code)
for name, method, path, body in [
    ("reading the credential secret", "GET", f"/api/v1/namespaces/{NS}/secrets/adbc-mesh-postgres", None),
    ("listing secrets", "GET", f"/api/v1/namespaces/{NS}/secrets", None),
    ("minting a token", "POST", f"/api/v1/namespaces/{NS}/serviceaccounts/spark-driver/token", {"spec": {"audiences": ["vault"]}}),
    ("exec into a pod", "POST", f"/api/v1/namespaces/{NS}/pods/x/exec?command=sh", None),
    ("granting itself a role", "POST", f"/apis/rbac.authorization.k8s.io/v1/namespaces/{NS}/rolebindings", {}),
    ("reading another namespace", "GET", "/api/v1/namespaces/relay-test/pods", None),
    ("changing a pod", "PATCH", f"/api/v1/namespaces/{NS}/pods/x", {}),
]:
    code, text = api(method, path, body)
    check(f"{name} is forbidden", code == 403, code)
secret_volume = {"name": "s", "secret": {"secretName": "adbc-mesh-postgres"}}
for name, want, body in [
    ("pod mounting the credential secret", "only an injected relay pod", pod("a1", {"volumes": [secret_volume]})),
    ("pod minting a vault token", "vault audience", pod("a2", {"volumes": [{"name": "t", "projected": {"sources": [{"serviceAccountToken": {"path": "t", "audience": "vault"}}]}}]})),
    ("privileged pod", "privileged", pod("a3", {"containers": [{"name": "app", "image": "evil", "securityContext": {"privileged": True}}]})),
    ("hostPath pod", "hostPath|host paths", pod("a4", {"volumes": [{"name": "h", "hostPath": {"path": "/"}}]})),
    ("hostNetwork pod", "hostNetwork|host namespaces", pod("a5", {"hostNetwork": True})),
    ("pod with an unapproved service account", "approved service account", pod("a6", {"serviceAccountName": "some-powerful-account"})),
    ("relay pod running its own sidecar", "Duplicate value", pod("a7", {"containers": [{"name": "relay-sidecar", "image": "evil"}]}, {"adbc-mesh.io/backend": "postgres"})),
]:
    code, text = dry_create(body)
    check(f"{name} is refused", code >= 400 and re.search(want, text) is not None, f"{code} {'' if code >= 400 and re.search(want, text) else text[:200]}")
code, text = dry_create(pod("ok1", {}))
check("ordinary pod is allowed, so the refusals above mean something", code in (200, 201), code if code < 300 else text[:140])

spark.stop()
print(f"RESULT {failures} failure(s)", flush=True)
sys.exit(1 if failures else 0)
