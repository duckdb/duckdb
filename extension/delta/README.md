# DuckDB Delta Extension
This is the experimental DuckDB extension for [Delta](https://delta.io/). It is built using the (also experimental)
[Delta Kernel](https://github.com/delta-incubator/delta-kernel-rs). The extension (currently) offers **read** support for delta
tables, both local and remote.

# Supported platforms
The supported platforms are:
- `linux_amd64`
- `osx_amd64` and `osx_arm64`

Support for the [other](https://duckdb.org/docs/stable/extensions/extension_distribution#platforms) DuckDB platforms is
work-in-progress

# How to use
**NOTE: this extension requires the DuckDB v0.10.3 or higher**

This extension is distributed as a binary extension. To use it, simply use one of its functions from DuckDB and the extension will be autoloaded:
```SQL
FROM delta_scan('s3://some/delta/table');
```

Note that using DuckDB [Secrets](https://duckdb.org/docs/stable/configuration/secrets_manager) for S3 authentication is supported:

```SQL
CREATE SECRET (TYPE S3, provider credential_chain);
FROM delta_scan('s3://some/delta/table/with/auth');
```

To scan a local table, use the full path prefixes with `file://`
```SQL
FROM delta_scan('file:///some/path/on/local/machine');
```

# Features
While still experimental, many (scanning) features/optimizations are already supported in this extension as it reuses most of DuckDB's
regular parquet scanning logic:
- multithreaded scans and parquet metadata reading
- data skipping/filter pushdown
  - skipping row-groups in file (based on parquet metadata)
  - skipping complete files (based on delta partition info)
- projection pushdown
- scanning tables with deletion vectors
- all primitive types
- structs
- S3 support with secrets

## Delta Lake V2 Features

The following Delta Lake V2 features are now supported:

### Deletion Vectors (Full Read Support)
Tables using deletion vectors are fully supported. The extension automatically filters out deleted rows during scanning.

### Row Tracking
Expose row tracking metadata as virtual columns:

```SQL
-- Get base row IDs for CDC/audit purposes
FROM delta_scan('path/to/table', delta_base_row_id=true);

-- Get row commit versions
FROM delta_scan('path/to/table', delta_row_commit_version=true);
```

### Deletion Vector Information
```SQL
-- Get deletion vector cardinality per file
FROM delta_scan('path/to/table', delta_dv_cardinality=true);
```

### Liquid Clustering Metadata
Liquid clustering configuration is parsed and available through the `delta_table_info` function.

### V2 Checkpoints
V2 checkpoint formats (UUID checkpoints, multi-part checkpoints, sidecar files) are supported through delta-kernel-rs.

## Metadata Inspection Functions

### delta_table_info
Returns table-level metadata including V2 feature flags:

```SQL
SELECT * FROM delta_table_info('path/to/table');
```

Returns columns:
- `path` - Table path
- `version` - Current snapshot version
- `min_reader_version`, `min_writer_version` - Protocol versions
- `has_deletion_vectors`, `has_row_tracking`, `has_liquid_clustering`, `has_v2_checkpoints`, `has_timestamp_ntz`, `has_column_mapping` - V2 feature flags
- `clustering_columns` - Array of liquid clustering column names
- `total_files`, `files_with_deletion_vectors`, `total_deleted_rows` - Statistics

### delta_file_stats
Returns per-file metadata including deletion vector and row tracking information:

```SQL
SELECT * FROM delta_file_stats('path/to/table');
```

Returns columns:
- `file_path`, `file_number`, `file_size` - Basic file info
- `has_deletion_vector`, `deleted_row_count`, `dv_storage_type`, `dv_size_bytes` - Deletion vector info
- `base_row_id`, `default_row_commit_version` - Row tracking info
- `partition_values` - Partition column values as MAP

More features coming soon!

# Building
See the [Extension Template](https://github.com/duckdb/extension-template) for generic build instructions

# Running tests
There are various tests available for the delta extension:
1. Delta Acceptence Test (DAT) based tests in `/test/sql/dat`
2. delta-kernel-rs based tests in `/test/sql/delta_kernel_rs`
3. Generated data based tests in `tests/sql/generated` (generated using [delta-rs](https://delta-io.github.io/delta-rs/), [PySpark](https://spark.apache.org/docs/latest/api/python/index.html), and DuckDB)

To run the first 2 sets of tests:
```shell
make test_debug
```
or in release mode
```shell
make test
```

To also run the tests on generated data:
```shell
make generate-data
GENERATED_DATA_AVAILABLE=1 make test
```