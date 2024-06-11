#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/types/value.hpp"

using duckdb::DBConfig;
using duckdb::Value;

// config
duckdb_state duckdb_create_config(duckdb_config *out_config) {
	if (!out_config) {
		return DuckDBError;
	}
	try {
		*out_config = nullptr;
		auto config = new DBConfig();
		*out_config = reinterpret_cast<duckdb_config>(config);
		config->SetOptionByName("duckdb_api", "capi");
	} catch (...) { // LCOV_EXCL_START
		return DuckDBError;
	} // LCOV_EXCL_STOP
	return DuckDBSuccess;
}

size_t duckdb_config_count() {
	return DBConfig::GetOptionCount();
}

duckdb_state duckdb_get_config_flag(size_t index, const char **out_name, const char **out_description) {
	auto option = DBConfig::GetOptionByIndex(index);
	if (!option) {
		return DuckDBError;
	}
	if (out_name) {
		*out_name = option->name;
	}
	if (out_description) {
		*out_description = option->description;
	}
	return DuckDBSuccess;
}

duckdb_state duckdb_set_config(duckdb_config config, const char *name, const char *option) {
	if (!config || !name || !option) {
		return DuckDBError;
	}

	try {
		auto db_config = (DBConfig *)config;
		db_config->SetOptionByName(name, Value(option));
	} catch (...) {
		return DuckDBError;
	}
	return DuckDBSuccess;
}

void duckdb_destroy_config(duckdb_config *config) {
	if (!config) {
		return;
	}
	if (*config) {
		auto db_config = (DBConfig *)*config;
		delete db_config;
		*config = nullptr;
	}
}
