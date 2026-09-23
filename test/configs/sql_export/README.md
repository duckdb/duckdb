The SQL export verifier is opt-in. `debug_verify_sql_export` defaults to `off`.
`report` executes generated SQL after a successful round trip and uses the original
plan for deliberately classified failures. `supported` permits fallback only for explicitly unsupported cases and fails verifier defects.
`strict` rejects all export failures. Thrown
extension, binding, and optimizer exceptions retain their normal propagation.
Explicit prepare/execute and parameterized queries are outside the initial envelope.

Run the frozen normal SQL corpus with its existing expected results:

```sh
python3 scripts/sql_export_corpus.py run --mode report \
  --manifest test/configs/sql_export/report_manifest.txt \
  --output build/sql_export_report
```

Each file runs in a separate process with the runner's own temporary directory.
The default external timeout is 60 seconds per file, including planning; adjust it
with `--timeout`. `--workers` controls independent file processes. The output
contains invocation and build provenance, exact commands and durations, raw test
events, a deterministically ordered `records.jsonl`, and `aggregate.json`. Test
requirement skips, timeouts, and assertion failures are separate from query coverage.
Occurrence coverage inventories the entire original plan before export blockers.
A successful round trip followed by an expected execution error still contributes
to round-trip coverage; report fallback never does.

Run the measured subset of unchanged tests strictly:

```sh
python3 scripts/sql_export_corpus.py run --mode strict \
  --manifest test/configs/sql_export/strict_manifest.txt \
  --output build/sql_export_strict
python3 scripts/sql_export_corpus.py selftest --output build/sql_export_runner_checks
```

Strict verification failures bypass ordinary expected-error matching. Each strict
file must also have positive eligible and generated counts; unavailable requirements
cannot make the corpus command pass.

CI runs `verify_sql_export_supported.json` as part of the Query Verification
configuration matrix. New SQL tests are included automatically unless explicitly
excluded. Queries outside the export envelope run normally; supported queries
execute their generated SQL against the existing test oracle. Verification defects
fail even statements expecting a SQL error.

The same CI job runs `make test_sql_export`, which requires successful round trips
for the strict manifest and runs the corpus runner selftests. Its coverage records
and logs are uploaded as the `sql-export-coverage` artifact.

Use `--failure-sql` on a corpus run to retain generated SQL in diagnostic records.
This includes successful query execution, since an expected-result mismatch is
only known after execution. SQL text is an optional reduction artifact, never a
stable coverage identity. Raw events always retain the original file and line.

After an intentional corpus update, `freeze` regenerates the report manifest.
`select-strict --aggregate <report>/aggregate.json` selects a small nonvacuous subset
whose report run had no eligible fallback or verifier failure. Review the selected
files before committing that manifest; do not change their expected outputs or
eligibility to improve coverage.
