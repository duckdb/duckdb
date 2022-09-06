//===----------------------------------------------------------------------===//
//                         DuckDB
//
// extension_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

struct ExtensionFunction {
	char extension[48];
	char function[48];
};

static constexpr ExtensionFunction EXTENSION_FUNCTIONS[] = {
    {"text", "excel"},
    {"excel_text", "excel"},
    {"drop_fts_index", "fts"},
    {"stem", "fts"},
    {"create_fts_index", "fts"},
    {"icu_calendar_names", "icu"},
    {"pg_timezone_names", "icu"},
    {"icu_sort_key", "icu"},
    {"make_timestamptz", "icu"},
    {"from_json_strict", "json"},
    {"json_group_structure", "json"},
    {"read_json_objects", "json"},
    {"->>", "json"},
    {"json_quote", "json"},
    {"array_to_json", "json"},
    {"json_array", "json"},
    {"json_type", "json"},
    {"json_extract_path", "json"},
    {"json_extract_string", "json"},
    {"read_ndjson_objects", "json"},
    {"json_array_length", "json"},
    {"json_transform_strict", "json"},
    {"from_json", "json"},
    {"json_merge_patch", "json"},
    {"json", "json"},
    {"json_object", "json"},
    {"json_structure", "json"},
    {"row_to_json", "json"},
    {"json_extract", "json"},
    {"json_group_array", "json"},
    {"json_transform", "json"},
    {"to_json", "json"},
    {"json_group_object", "json"},
    {"json_extract_path_text", "json"},
    {"json_valid", "json"},
    {"parquet_scan", "parquet"},
    {"parquet_metadata", "parquet"},
    {"parquet_schema", "parquet"},
    {"read_parquet", "parquet"},
    {"tpcds_answers", "tpcds"},
    {"dsdgen", "tpcds"},
    {"tpcds", "tpcds"},
    {"tpcds_queries", "tpcds"},
    {"tpch_answers", "tpch"},
    {"tpch", "tpch"},
    {"dbgen", "tpch"},
    {"tpch_queries", "tpch"},
    {"visualize_json_profiling_output", "visualizer"},
    {"visualize_last_profiling_output", "visualizer"},
    {"visualize_diff_profiling_output", "visualizer"},
    {"sqlite_attach", "sqlite_scanner"},
    {"sqlite_scan", "sqlite_scanner"},
    {"postgres_attach", "postgres_scanner"},
    {"postgres_scan_pushdown", "postgres_scanner"},
    {"postgres_scan", "postgres_scanner"},
    {"get_substrait_json", "substrait"},
    {"from_substrait", "substrait"},
    {"get_substrait", "substrait"},
};
} // namespace duckdb
