# Non-dictionary Bloom filters

Parquet COPY retains its existing dictionary-based Bloom filters by default. `BLOOM_FILTER_FALLBACK_COLUMNS` opts selected top-level columns into additional filters when a column chunk does not use dictionary encoding:

```sql
COPY events TO 'events.parquet' (
    FORMAT PARQUET,
    BLOOM_FILTER_FALLBACK_COLUMNS ['trace_id', 'request_id'],
    BLOOM_FILTER_FALLBACK_MAX_BYTES 67108864
);
```

Names match output columns case-insensitively. An empty list selects nothing. Dots are literal parts of a column name, not nested-field paths. Unknown names, duplicate names, nested types, and unsupported scalar types are rejected.

The supported types are signed and unsigned integers through 64 bits, FLOAT, DOUBLE, VARCHAR, BLOB, UUID, DATE, TIME/TIME_NS/TIME_TZ, INTERVAL, timestamp types written as INT64, and DECIMAL with precision at most 18. Boolean, enum, huge integer, larger decimal, and INT96 timestamp fallback filters are not supported. An empty or all-null non-dictionary chunk needs no filter.

Retained dictionaries still use their distinct-entry count and existing insertion path. `WRITE_BLOOM_FILTER FALSE` disables both paths. Encrypted output does not get fallback filters.

## Sizing and memory

The fallback uses the non-null row count as an upper bound on distinct values. It hashes values during the write pass, then folds adjacent split blocks by OR when an average-occupancy estimate permits halving the filter. The result is a standard Parquet Split Block Bloom Filter.

`BLOOM_FILTER_FALSE_POSITIVE_RATIO` controls this estimate. It is not a measured upper bound: the occupancy approximation and existing dictionary-sizing formula can both exceed the requested rate for some distributions and size boundaries. Folding preserves membership of inserted values.

`BLOOM_FILTER_FALLBACK_MAX_BYTES` defaults to **64 MiB per output file**. It bounds fallback bitset allocations, including:

* Filters being populated by concurrent row-group writers.
* Filters retained until the footer is written.
* Scratch allocation used to compact a folded filter.

Construction reserves twice the initial bitset size. After folding/compaction, the reservation shrinks to the retained allocation. If the initial reservation does not fit, the writer omits that chunk's fallback before hashing values. Filters are never truncated to fit the budget. The initial bitset is also limited to 128 MiB; an excessive size request is skipped before integer conversion. Zero disables additional filters without disabling existing dictionary filters.

This is a bound on additional Bloom bitsets, not all writer memory. Existing dictionary filters, column buffers, metadata, and allocator overhead are outside it. Multiple files/COPY operations have independent budgets. Near the limit, parallel scheduling can affect which chunks receive a filter.

## Choosing columns

Select columns used for equality or IN lookups whose values are scattered across pages but occur in relatively few row groups, such as random trace IDs. Bloom filters reject entire row groups; they cannot locate a matching page or directly answer a range predicate.

Sorted or clustered keys often already benefit from min/max statistics and page indexes. Repeated values present in every group cannot benefit from Bloom pruning for present-value lookups. These are workload choices, not automatic heuristics: random integers may benefit, sorted strings may not, and absent-value probes can still benefit on either.

Adding fallback filters increases write work, temporary memory, and file size. Folding reduces the additional bytes for repeated values, but cannot recover the hashing work or initial allocation. Unselected columns preserve the previous policy.
