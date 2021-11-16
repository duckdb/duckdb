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

struct MaximumMemorySetting {
	static constexpr const char *Name = "max_memory";
	static constexpr const char *Description = "The maximum memory of the system (e.g. 1GB)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
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
