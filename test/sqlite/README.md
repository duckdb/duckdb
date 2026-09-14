# SQLLogic Test Runner

## Origin

Here you'll find source code originating from
[SQLite's SQLLogicTest](https://sqlite.org/sqllogictest/doc/trunk/about.wiki).
DuckDB has extended functionality in several ways, including several new expressions
(test_env, set/reset, tags).

## Usage Notes

### Environment: test_env and require-env

Environment variables can be managed in 2 ways: `test_env` which allows variables to have defaults set, and `require-env` which is a select/skip predicate for a test file.

For examples of `test_env` usage see the `duckdb/ducklake` extension tests.

When a file `require-env FOO`, or `require-env FOO=bar` a test will only execute if FOO is set, or in the latter case, set to `bar`.

### Tags: explicit and implicit

SQL test files also support a `tags` attribute of the form:

```text
tags optimization memory>=64GB
```

The tags are free-form, and can be used when executing tests for both selection and skipping, a la:

```bash
build/release/test/unittest --skip-tag 'slow' --select-tag-set "['memory>=64GB', 'env[TEST_DATA]']"
```

Tags can be specified individually, or as a set (which is treated as an `AND` predicate).
Each specification is an `OR`, and selects are processed before skips.

Additionally some implicit tags are computed when an SQL test file is parsed.
All `require-env` and `test_env` expressions will be added as tags of the form `env[VAR]`, and
`env[VAR]=VALUE` (when specified).

For an extensive example of tag matching expectations, see the file
`test/sqlite/validate_tags_usage.sh` which unit tests these behaviors.

### Executing SQL from EXPLAIN

The `explain_sql` modifier applies to the following `query` or `statement` command:

```text
explain_sql

query I
SELECT 1 + 2;
----
3
```

The runner obtains `EXPLAIN (SQL)` on the command's connection, checks its result shape, and executes the returned query once against the existing expected-result oracle. The original query is prepared for an exact output-name/type comparison but is not executed. A preparation failure in either form fails this schema check. The modifier requires one query; use an ordinary command to test an EXPLAIN error. An unexpected EXPLAIN failure cannot satisfy `statement error`, which checks errors from the generated execution.

The debug SQL-export verifier is temporarily disabled and restored, including whether its setting was inherited. This prevents another round trip from replacing the SQL under test. With SQL-export events enabled, these executions emit separate `explain_sql` records; they do not count toward the internal verifier's coverage denominator.
