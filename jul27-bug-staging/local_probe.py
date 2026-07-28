#!/usr/bin/env python3
import os
import subprocess
import textwrap
import time

ROOT = "/Users/hjiang/Desktop/duckdb"
DUCKDB = os.path.join(ROOT, "build", "reldebug", "duckdb")
OUT = os.path.join(ROOT, "jul27-bug-staging", "local_probe")
os.makedirs(OUT, exist_ok=True)

CASES = {
    "nested_window_subquery": r"""
        PRAGMA threads=4;
        CREATE TABLE t AS
        SELECT i, i % 7 AS g, [i, i + 1, NULL] AS l, {'a': i, 'b': i % 3} AS s
        FROM range(0, 5000) tbl(i);
        SELECT sum(x)
        FROM (
          SELECT i, g, row_number() OVER (PARTITION BY g ORDER BY s.a DESC) AS rn,
                 list_extract(l, 2) AS x
          FROM t
          WHERE i IN (SELECT i FROM t WHERE g BETWEEN 2 AND 5)
        )
        WHERE rn <= 10;
    """,
    "rowgroup_pruning_update_delete": r"""
        PRAGMA threads=4;
        CREATE TABLE rg AS SELECT i, i::VARCHAR AS payload FROM range(0, 200000) tbl(i);
        CHECKPOINT;
        DELETE FROM rg WHERE i BETWEEN 50000 AND 150000;
        UPDATE rg SET payload = 'hit' WHERE i IN (1, 199999);
        SELECT count(*), min(i), max(i), sum(payload = 'hit') FROM rg WHERE i >= 0;
        SELECT count(*) FROM rg WHERE i BETWEEN 50000 AND 150000;
    """,
    "catalog_temp_attach_view_macro": r"""
        CREATE TEMP TABLE temp_t AS SELECT 42 AS a;
        CREATE MACRO m(x) AS (x + (SELECT max(a) FROM temp_t));
        CREATE VIEW v AS SELECT m(a) AS y FROM temp_t;
        SELECT * FROM v;
        DROP TABLE temp_t;
        SELECT * FROM v;
    """,
    "compression_checkpoint_strings": r"""
        PRAGMA threads=4;
        CREATE TABLE c AS
        SELECT i, repeat(chr(65 + (i % 26)), 100) AS s
        FROM range(0, 100000) tbl(i);
        CHECKPOINT;
        SELECT count(*), min(s), max(s), sum(length(s)) FROM c WHERE s >= 'M';
    """,
    "decorrelate_list_struct": r"""
        CREATE TABLE a AS SELECT i, {'k': i % 10, 'v': [i, i+1]} AS s FROM range(0, 1000) tbl(i);
        CREATE TABLE b AS SELECT i % 10 AS k, count(*) AS c FROM range(0, 1000) tbl(i) GROUP BY 1;
        SELECT count(*)
        FROM a
        WHERE EXISTS (
          SELECT 1 FROM b
          WHERE b.k = a.s.k AND b.c > list_extract(a.s.v, 1)
        );
    """,
}

def run_case(name, sql):
    db = os.path.join(OUT, f"{name}.duckdb")
    log = os.path.join(OUT, f"{name}.log")
    script = os.path.join(OUT, f"{name}.sql")
    with open(script, "w") as f:
        f.write(textwrap.dedent(sql).strip() + "\n")
    start = time.time()
    proc = subprocess.run([DUCKDB, db, "-batch", "-init", script], capture_output=True, text=True, timeout=60)
    elapsed = time.time() - start
    with open(log, "w") as f:
        f.write(f"case={name}\nreturncode={proc.returncode}\nelapsed={elapsed:.3f}s\n")
        f.write("\nSTDOUT\n")
        f.write(proc.stdout)
        f.write("\nSTDERR\n")
        f.write(proc.stderr)
    return name, proc.returncode, elapsed, proc.stdout, proc.stderr

def main():
    summary = []
    for name, sql in CASES.items():
        try:
            summary.append(run_case(name, sql))
        except Exception as exc:
            summary.append((name, -999, 0, "", repr(exc)))
    with open(os.path.join(OUT, "summary.md"), "w") as f:
        f.write("# Local Probe Summary\n\n")
        for name, code, elapsed, stdout, stderr in summary:
            status = "interesting" if code != 0 or "INTERNAL" in stderr or "INTERNAL" in stdout else "ok"
            f.write(f"- `{name}`: {status}, rc={code}, elapsed={elapsed:.3f}s\n")
            if status == "interesting":
                f.write(f"  - stderr: `{stderr.strip()[:300]}`\n")

if __name__ == "__main__":
    main()
