# PEG Parser Strict Mode — Failing Tests

**Config:** `test/configs/peg_parser_strict.json`
**Total tests:** 3937 | **Failing:** 94

All failures are "error message did not match" — the PEG parser throws an error but produces a different message (or points to a different token) than what the test expects.

---

## Category 1: Lambda / List Comprehension (8 tests)

All lambda tests fail because the PEG parser reports `syntax error at or near "AS"`, `"["`, or `","` where the old parser had more specific/different error messages.

### Deprecated Arrow (`->`) Syntax (3 tests)
| # | Test | Expected Error |
|---|------|----------------|
| 52 | `test/sql/function/list/lambdas/arrow/reduce_deprecated.test:83` | `syntax error at or near "AS"` |
| 53 | `test/sql/function/list/lambdas/arrow/reduce_initial_deprecated.test:87` | `syntax error at or near "AS"` |
| 54 | `test/sql/function/list/lambdas/arrow/list_comprehension_deprecated.test:96` | `syntax error at or near "["` |

### Current Lambda Syntax (5 tests)
| # | Test | Expected Error |
|---|------|----------------|
| 55 | `test/sql/function/list/lambdas/incorrect.test:251` | `Parser Error.*syntax error.*` |
| 56 | `test/sql/function/list/lambdas/reduce_initial.test:84` | `syntax error at or near "AS"` |
| 57 | `test/sql/function/list/lambdas/list_comprehension.test:86` | `syntax error at or near "["` |
| 58 | `test/sql/function/list/lambdas/lambda_scope.test:50` | `Parser Error.*syntax error at or near.*` |
| 59 | `test/sql/function/list/lambdas/reduce.test:80` | `syntax error at or near ","` |

---

## Category 2: PRAGMA Parsing (5 tests)

PEG parser produces different errors for malformed PRAGMA statements.

| # | Test | Expected Error |
|---|------|----------------|
| 3 | `test/fuzzer/public/pragma_named_parameters.test:8` | `PRAGMA statement with assignment cannot have named parameters` |
| 48 | `test/sql/pragma/test_memory_limit.test:125` | `syntax error at or near ")"` |
| 49 | `test/sql/pragma/test_pragma_database_list.test:21` | `Parser Error.*syntax error.*` |
| 50 | `test/sql/pragma/test_enable_profile.test:6` | `syntax error at or near ")"` |
| 51 | `test/sql/pragma/test_pragma_parsing.test:6` | `syntax error at or near "PRAG"` |

---

## Category 3: CREATE Statements (7 tests)

| # | Test | Expected Error |
|---|------|----------------|
| 86 | `test/sql/create/create_table_as_error.test:8` | `syntax error at or near ")"` |
| 87 | `test/sql/create/create_table_compression.test:25` | `Parser Error: syntax error.*` |
| 88 | `test/sql/create/create_or_replace.test:28` | `syntax error at or near "IF"` |
| 89 | `test/sql/create/create_as.test:60` | `UNIQUE USING INDEX is not supported` |
| 90 | `test/sql/create/create_using_index.test:5` | `syntax error at or near "DATABASE"` |
| 91 | `test/sql/create/create_database.test:5` | `zero-length delimited identifier at or near """"` |
| 92 | `test/sql/create/create_table_empty_name.test:12` | `syntax error at or near "SELEC"` |

---

## Category 4: ALTER Statements (5 tests)

| # | Test | Expected Error |
|---|------|----------------|
| 68 | `test/sql/alter/test_alter_database_rename.test:38` | `Adding columns is only supported for tables` |
| 69 | `test/sql/alter/add_col/test_add_col_incorrect.test:19` | `syntax error at or near "EXISTS"` |
| 70 | `test/sql/alter/test_alter_if_exists.test:89` | `Not implemented Error: Altering schemas is not yet supported` |
| 71 | `test/sql/alter/rename_schema/rename_schema.test:5` | `Parser Error.*"'baz'".*` OR `Reset option "foo" cannot set any value.*` |
| 72 | `test/sql/alter/alter_table_set_table_options.test:38` | `syntax error at or near "SELEC"` |

---

## Category 5: SELECT Variants (4 tests)

| # | Test | Expected Error |
|---|------|----------------|
| 80 | `test/sql/select/test_select_into.test:12` | `Parser Error.*SELECT INTO not supported.*` |
| 81 | `test/sql/select/test_select_alias_prefix_colon.test:85` | `syntax error` |
| 82 | `test/sql/select/test_select_locking.test:9` | `Parser Error.*SELECT locking clause is not supported.*` |
| 83 | `test/sql/select/test_positional_reference.test:44` | `Parser Error.*Positional reference node needs.*` |

---

## Category 6: Type System (10 tests)

Covers decimal, interval, date, enum, struct, union, array, and nested list types.

| # | Test | Expected Error |
|---|------|----------------|
| 15 | `test/sql/types/decimal/test_decimal.test:166` | `Parser Error.*syntax error.*` |
| 16 | `test/sql/types/interval/interval_constants.test:83` | `Parser Error.*not supported.*` |
| 17 | `test/sql/types/date/date_parsing.test:373` | `Parser Error.*syntax error.*` |
| 18 | `test/sql/types/enum/standalone_enum.test:36` | `syntax error at or near "42"` |
| 19 | `test/sql/types/struct/update_empty_row.test:5` | `Parser Error.*expected 1 values, got 0.*` |
| 20 | `test/sql/types/struct/unnest_column_names.test:86` | `Parser Error: syntax error` |
| 21 | `test/sql/types/union/union_limits.test:58` | `Parser Error.*syntax error.*` |
| 22 | `test/sql/types/nested/array/array_invalid.test:38` | `syntax error at or near "-"` |
| 23 | `test/sql/types/nested/struct/struct_dict.test:43` | `Parser Error.*syntax error at or near "}".*` |
| 24 | `test/sql/types/nested/list/test_nested_list.test:247` | `Parser Error.*syntax error.*` |

---

## Category 7: Catalog (8 tests)

Covers `COMMENT ON`, sequences, ownership/dependencies, and macro/query functions.

| # | Test | Expected Error |
|---|------|----------------|
| 39 | `test/sql/catalog/comment_on_wal.test:18` | `Parser Error: syntax error` |
| 40 | `test/sql/catalog/comment_on.test:10` | `Parser Error: syntax error` |
| 41 | `test/sql/catalog/dependencies/test_alter_dependency_ownership.test:102` | `Owned by value should be passed as most once` |
| 42 | `test/sql/catalog/function/query_function.test:95` | `syntax error at or near "FROM"` |
| 43 | `test/sql/catalog/function/test_simple_macro.test:337` | `syntax error` |
| 44 | `test/sql/catalog/comment_on_extended.test:113` | `Invalid column reference: 'galaxy.db."schema"."table".col1', too many dots` |
| 45 | `test/sql/catalog/test_temporary.test:68` | `Not implemented Error: Only ON COMMIT.*` |
| 46 | `test/sql/catalog/sequence/test_sequence.test:533` | `Parser Error: Maxvalue should be passed as most once.*` |

---

## Category 8: Aggregate / Grouping / Window (5 tests)

| # | Test | Expected Error |
|---|------|----------------|
| 35 | `test/sql/cast/struct_to_map.test:43` | *(different failure type — no expected error text captured)* |
| 36 | `test/sql/aggregate/qualify/test_qualify.test:198` | `syntax error at or near "WINDOW"` |
| 37 | `test/sql/aggregate/grouping_sets/grouping.test:186` | `syntax error at or near ")"` |
| 38 | `test/sql/aggregate/grouping_sets/rollup.test:131` | `syntax error at or near ")"` |
| 77 | `test/sql/window/test_invalid_window.test:63` | `syntax error at or near "COMPRESSION"` |

---

## Category 9: ATTACH / Schema (3 tests)

| # | Test | Expected Error |
|---|------|----------------|
| 74 | `test/sql/attach/attach_cross_catalog.test:16` | `Expected "USE database" or "USE database.schema"` |
| 75 | `test/sql/attach/reattach_schema.test:77` | `Too many qualifications for type name` |
| 76 | `test/sql/attach/attach_enums.test:21` | `syntax error at or near` |

---

## Category 10: List Functions (3 tests)

| # | Test | Expected Error |
|---|------|----------------|
| 60 | `test/sql/function/list/list_reverse.test:134` | `syntax error at or near ")"` |
| 61 | `test/sql/function/list/list_position.test:276` | `syntax error at or near ")"` |
| 62 | `test/sql/function/list/list_contains.test:224` | `Parser Error: syntax error.*` |

---

## Category 11: String Functions (2 tests)

| # | Test | Expected Error |
|---|------|----------------|
| 63 | `test/sql/function/string/test_similar_to.test:134` | `syntax error at or near ")"` |
| 64 | `test/sql/function/string/test_trim.test:133` | `Parser Error.*syntax error.*` |

---

## Category 12: CTE (2 tests)

| # | Test | Expected Error |
|---|------|----------------|
| 84 | `test/sql/cte/insert_cte_bug_3417.test:14` | `Parser Error: exprLocation NOT IMPLEMENTED` |
| 85 | `test/sql/cte/cte_describe.test:26` | `Parser Error.*A CTE needs a SELECT.*` |

---

## Category 13: Prepared Statements (2 tests)

| # | Test | Expected Error |
|---|------|----------------|
| 25 | `test/sql/prepared/prepared_named_param.test:12` | `Not implemented Error: Mixing named parameters and positional parameters is not supported yet` |
| 26 | `test/sql/prepared/test_basic_prepare.test:42` | `syntax error at or near "SELECT"` |

---

## Category 14: Parser / Error Handling (5 tests)

Tests for parser-level error messages and error formatting.

| # | Test | Expected Error |
|---|------|----------------|
| 65 | `test/sql/parser/invisible_spaces.test:57` | `Parser Error.*syntax error at or near.*` |
| 66 | `test/sql/parser/test_long_error.test:5` | `syntax error at or near "="` |
| 67 | `test/sql/peg_parser/enable_peg_parser.test:7` | `syntax error at or near "DATABSE"` (typo detection) |
| 93 | `test/sql/error/incorrect_sql.test:6` | `Parser Error: SELECT clause without selection list` |
| 94 | `test/sql/projection/test_scalar_projection.test:49` | *(no expected error captured)* |

---

## Category 15: Miscellaneous SQL (15 tests)

Various SQL features that don't fit neatly into other categories.

| # | Test | Expected Error |
|---|------|----------------|
| 8 | `test/sql/order/test_order_pragma.test:36` | `Parser Error.*syntax error.*` |
| 9 | `test/sql/order/test_nulls_first.test:119` | `Parser Error.*syntax error.*` |
| 10 | `test/sql/settings/errors_as_json.test:21` | `Parser Error.*syntax error at or near.*` |
| 11 | `test/sql/setops/test_pg_union.test:341` | `Parser Error.*SELECT locking clause is not supported.*` |
| 12 | `test/sql/pivot/test_pivot.test:262` | `syntax error at or near ")"` |
| 13 | `test/sql/pivot/test_unpivot.test:164` | `syntax error` |
| 14 | `test/sql/returning/returning_insert.test:142` | `Parser Error.*syntax error.*` |
| 27 | `test/sql/join/inner/test_using_join.test:59` | `syntax error at or near "+"` |
| 28 | `test/sql/generated_columns/virtual/default.test:18` | `syntax error at or near "GENERATED"` |
| 29 | `test/sql/update/test_multiple_assignment.test:73` | `syntax error at or near ")"` |
| 30 | `test/sql/update/update_error_suggestions.test:16` | `Qualified column names in UPDATE .. SET not supported` |
| 31 | `test/sql/collate/test_collate_orderby_column_number.test:25` | `syntax error at or near "ORDER"` |
| 32 | `test/sql/binder/alias_qualification_errors.test:24` | `syntax error at or near ".123"` |
| 33 | `test/sql/binder/test_string_alias.test:33` | `Parser Error: syntax error` |
| 34 | `test/sql/subquery/any_all/test_any_all.test:84` | `ANY and ALL operators require one of =,<>,>,<,>=,<= comparisons!` |
| 47 | `test/sql/upsert/upsert_conflict_target_index.test:30` | `Not implemented Error: ON CONSTRAINT conflict target is not supported yet` |
| 73 | `test/sql/transactions/test_transaction_abort.test:20` | `syntax error` |
| 78 | `test/sql/copy/csv/test_compression_flag.test:55` | `syntax error at or near '...'` |
| 79 | `test/sql/copy/csv/glob/copy_csv_glob.test:29` | `Parser Error.*SELECT INTO not supported.*` |

---

## Category 16: Fuzzer / Regression Issues (6 tests)

| # | Test | Expected Error |
|---|------|----------------|
| 1 | `test/extension/consistent_semicolon_extension_parse.test:8` | `This is not a quack: (foo);` (extension custom parser error) |
| 2 | `test/fuzzer/pedro/window_self_reference.test:8` | `window functions are not allowed in window definitions` |
| 4 | `test/fuzzer/duckfuzz/arrow_scan_subquery_nullptr.test:5` | `Parser Error: syntax error at or near "Binder"` (multi-line error as SQL) |
| 5 | `test/fuzzer/duckfuzz/regex_syntax_2889.test:25` | `SELECT clause without selection list` |
| 6 | `test/issues/general/test_4008.test:8` | `Parser Error.*operators require.*comparisons.*` |
| 7 | `test/issues/rigger/test_507.test:9` | `Parser Error:.*at least one column.*` |

---

## Summary by Category

| Category | Count |
|----------|-------|
| Lambda / List Comprehension | 8 |
| PRAGMA Parsing | 5 |
| CREATE Statements | 7 |
| ALTER Statements | 5 |
| SELECT Variants | 4 |
| Type System | 10 |
| Catalog | 8 |
| Aggregate / Grouping / Window | 5 |
| ATTACH / Schema | 3 |
| List Functions | 3 |
| String Functions | 2 |
| CTE | 2 |
| Prepared Statements | 2 |
| Parser / Error Handling | 5 |
| Miscellaneous SQL | 19 |
| Fuzzer / Regression | 6 |
| **Total** | **94** |

---

## Common Root Causes

1. **Error message text changed**: PEG parser uses different wording for the same invalid construct (e.g., points to a different token, or uses a different message entirely). This accounts for the majority of failures.

2. **Not implemented in PEG parser**: A handful of tests expect a specific `Not implemented Error` that the PEG parser no longer produces with the same message — e.g., `ON CONSTRAINT` conflict target (#47), `ON COMMIT` (#45), altering schemas (#70), mixing named/positional parameters (#25).

3. **exprLocation not implemented** (#84): PEG parser emits a literal `exprLocation NOT IMPLEMENTED` string — indicates an incomplete code path.

4. **Extension custom parser** (#1): The custom extension parser error message is no longer surfaced through the PEG parser path.

5. **Deprecated arrow lambda syntax** (#52–54): Arrow (`->`) lambda syntax errors are reported differently by the PEG parser.
