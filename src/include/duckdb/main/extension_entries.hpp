//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension_entries.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

struct ExtensionEntry {
	char name[48];
	char extension[48];
};

static constexpr ExtensionEntry EXTENSION_FUNCTIONS[] = {
    {"->>", "json"},
    {"array_to_json", "json"},
    {"create_fts_index", "fts"},
    {"current_localtime", "icu"},
    {"current_localtimestamp", "icu"},
    {"dbgen", "tpch"},
    {"drop_fts_index", "fts"},
    {"dsdgen", "tpcds"},
    {"excel_text", "excel"},
    {"from_json", "json"},
    {"from_json_strict", "json"},
    {"from_substrait", "substrait"},
    {"from_substrait_json", "substrait"},
    {"get_substrait", "substrait"},
    {"get_substrait_json", "substrait"},
    {"icu_calendar_names", "icu"},
    {"icu_sort_key", "icu"},
    {"json", "json"},
    {"json_array", "json"},
    {"json_array_length", "json"},
    {"json_contains", "json"},
    {"json_extract", "json"},
    {"json_extract_path", "json"},
    {"json_extract_path_text", "json"},
    {"json_extract_string", "json"},
    {"json_group_array", "json"},
    {"json_group_object", "json"},
    {"json_group_structure", "json"},
    {"json_keys", "json"},
    {"json_merge_patch", "json"},
    {"json_object", "json"},
    {"json_quote", "json"},
    {"json_structure", "json"},
    {"json_transform", "json"},
    {"json_transform_strict", "json"},
    {"json_type", "json"},
    {"json_valid", "json"},
    {"make_timestamptz", "icu"},
    {"parquet_metadata", "parquet"},
    {"parquet_scan", "parquet"},
    {"parquet_schema", "parquet"},
    {"pg_timezone_names", "icu"},
    {"postgres_attach", "postgres_scanner"},
    {"postgres_scan", "postgres_scanner"},
    {"postgres_scan_pushdown", "postgres_scanner"},
    {"read_json", "json"},
    {"read_json_auto", "json"},
    {"read_json_objects", "json"},
    {"read_ndjson", "json"},
    {"read_ndjson_auto", "json"},
    {"read_ndjson_objects", "json"},
    {"read_parquet", "parquet"},
    {"row_to_json", "json"},
    {"scan_arrow_ipc", "arrow"},
    {"sqlite_attach", "sqlite_scanner"},
    {"sqlite_scan", "sqlite_scanner"},
    {"stem", "fts"},
    {"text", "excel"},
    {"to_arrow_ipc", "arrow"},
    {"to_json", "json"},
    {"tpcds", "tpcds"},
    {"tpcds_answers", "tpcds"},
    {"tpcds_queries", "tpcds"},
    {"tpch", "tpch"},
    {"tpch_answers", "tpch"},
    {"tpch_queries", "tpch"},
    {"visualize_diff_profiling_output", "visualizer"},
    {"visualize_json_profiling_output", "visualizer"},
    {"visualize_last_profiling_output", "visualizer"},
};

static constexpr ExtensionEntry EXTENSION_SETTINGS[] = {
    {"binary_as_string", "parquet"},
    {"calendar", "icu"},
    {"http_retries", "httpfs"},
    {"http_retry_backoff", "httpfs"},
    {"http_retry_wait_ms", "httpfs"},
    {"http_timeout", "httpfs"},
    {"force_download", "httpfs"},
    {"s3_access_key_id", "httpfs"},
    {"s3_endpoint", "httpfs"},
    {"s3_region", "httpfs"},
    {"s3_secret_access_key", "httpfs"},
    {"s3_session_token", "httpfs"},
    {"s3_uploader_max_filesize", "httpfs"},
    {"s3_uploader_max_parts_per_file", "httpfs"},
    {"s3_uploader_thread_limit", "httpfs"},
    {"s3_url_compatibility_mode", "httpfs"},
    {"s3_url_style", "httpfs"},
    {"s3_use_ssl", "httpfs"},
    {"sqlite_all_varchar", "sqlite_scanner"},
    {"timezone", "icu"},
};
} // namespace duckdb
