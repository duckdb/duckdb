//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/settings.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
class ClientContext;
class DatabaseInstance;
struct DBConfig;

struct AccessModeSetting {
	static constexpr const char *Name = "access_mode";
	static constexpr const char *Description = "Access mode of the database ([AUTOMATIC], READ_ONLY or READ_WRITE)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static Value GetSetting(ClientContext &context);
};

struct DefaultCollationSetting {
	static constexpr const char *Name = "default_collation";
	static constexpr const char *Description = "The collation setting used when none is specified";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void SetLocal(ClientContext &context, const Value &parameter);
	static Value GetSetting(ClientContext &context);
};

struct DefaultOrderSetting {
	static constexpr const char *Name = "default_order";
	static constexpr const char *Description = "The order type used when none is specified ([ASC] or DESC)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static Value GetSetting(ClientContext &context);
};

struct DefaultNullOrderSetting {
	static constexpr const char *Name = "default_null_order";
	static constexpr const char *Description = "Null ordering used when none is specified ([NULLS_FIRST] or NULLS_LAST)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static Value GetSetting(ClientContext &context);
};

struct EnableExternalAccessSetting {
	static constexpr const char *Name = "enable_external_access";
	static constexpr const char *Description = "Allow the database to access external state (through e.g. loading/installing modules, COPY TO/FROM, CSV readers, pandas replacement scans, etc)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static Value GetSetting(ClientContext &context);
};

struct EnableObjectCacheSetting {
	static constexpr const char *Name = "enable_object_cache";
	static constexpr const char *Description = "Whether or not object cache is used to cache e.g. Parquet metadata";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static Value GetSetting(ClientContext &context);
};

struct EnableProfilingSetting {
	static constexpr const char *Name = "enable_profiling";
	static constexpr const char *Description = "Enables profiling, and sets the output format (JSON, [QUERY_TREE], QUERY_TREE_OPTIMIZER)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static Value GetSetting(ClientContext &context);
};

struct ExplainOutputSetting {
	static constexpr const char *Name = "explain_output";
	static constexpr const char *Description = "Output of EXPLAIN statements (ALL, [OPTIMIZED_ONLY], PHYSICAL_ONLY)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static Value GetSetting(ClientContext &context);
};

struct LogQueryPathSetting {
	static constexpr const char *Name = "log_query_path";
	static constexpr const char *Description = "Specifies the path to which queries should be logged (default: empty string, queries are not logged)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static Value GetSetting(ClientContext &context);
};

struct MaximumMemorySetting {
	static constexpr const char *Name = "max_memory";
	static constexpr const char *Description = "The maximum memory of the system (e.g. 1GB)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static Value GetSetting(ClientContext &context);
};

struct ProfileOutputSetting {
	static constexpr const char *Name = "profile_output";
	static constexpr const char *Description = "The file to which profile output should be saved, or empty to print to the terminal";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static Value GetSetting(ClientContext &context);
};

struct ProfilingModeSetting {
	static constexpr const char *Name = "profiling_mode";
	static constexpr const char *Description = "The profiling mode ([standard] or detailed)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static Value GetSetting(ClientContext &context);
};

struct SchemaSetting {
	static constexpr const char *Name = "schema";
	static constexpr const char *Description = "Sets the default search schema. Equivalent to setting search_path to a single value.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static Value GetSetting(ClientContext &context);
};

struct SearchPathSetting {
	static constexpr const char *Name = "search_path";
	static constexpr const char *Description = "Sets the default search search path as a comma-separated list of values";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static Value GetSetting(ClientContext &context);
};

struct SetProfilerHistorySize {
	static constexpr const char *Name = "set_profiler_history_size";
	static constexpr const char *Description = "Sets the profiler history size";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BIGINT;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static Value GetSetting(ClientContext &context);
};

struct ThreadsSetting {
	static constexpr const char *Name = "threads";
	static constexpr const char *Description = "The number of total threads used by the system.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BIGINT;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static Value GetSetting(ClientContext &context);
};

} // namespace duckdb
