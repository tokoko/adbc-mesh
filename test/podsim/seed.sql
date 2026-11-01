CREATE TABLE employees (
  id bigint PRIMARY KEY, name text, dept text, active boolean, score double precision,
  hired timestamp, salary bigint, ssn text
);
INSERT INTO employees
SELECT i, 'user' || i, CASE WHEN i % 2 = 0 THEN 'eng' ELSE NULL END, i % 3 = 0, i * 0.5,
       TIMESTAMP '2020-01-01' + i * INTERVAL '1 minute', 100000 + i, 'SECRET-SSN-' || i
FROM generate_series(1, 2000000) i;
CREATE TABLE secrets (id bigint, token text);
INSERT INTO secrets VALUES (1, 'SECRET-TOKEN');
-- Enforcement now lives in the backend: the sidecar logs in as a role with column-level grants.
CREATE ROLE relay_reader LOGIN PASSWORD 's3cr3t-pg-password';
GRANT SELECT (id, name, dept, active, score, hired) ON employees TO relay_reader;
