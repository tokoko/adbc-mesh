"""Create default.relay_employees in the adbc-iceberg docker compose catalog."""
import numpy as np
import pyarrow as pa
from pyiceberg.catalog import load_catalog

catalog = load_catalog("rest", **{
    "uri": "http://localhost:8181", "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "admin", "s3.secret-access-key": "password", "s3.region": "us-east-1",
})
try:
    catalog.create_namespace("default")
except Exception:
    pass
n = 5_000_000
ids = np.arange(n, dtype=np.int64)
table = pa.table({
    "id": ids,
    "name": pa.array([f"user{i}" for i in range(n)]),
    "value": ids * 0.5,
    "salary": ids + 100000,
    "ssn": pa.array([f"SECRET-SSN-{i}" for i in range(n)]),
})
try:
    catalog.drop_table("default.relay_employees")
except Exception:
    pass
catalog.create_table("default.relay_employees", schema=table.schema).append(table)
print("seeded", n)
