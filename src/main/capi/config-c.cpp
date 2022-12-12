#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/cbuffer_manager.hpp"
#include "duckdb/common/helper.hpp"

using duckdb::DBConfig;
using duckdb::Value;

// config
duckdb_state duckdb_create_config(duckdb_config *out_config) {
	if (!out_config) {
		return DuckDBError;
	}
	DBConfig *config;
	try {
		config = new DBConfig();
	} catch (...) { // LCOV_EXCL_START
		return DuckDBError;
	} // LCOV_EXCL_STOP
	*out_config = (duckdb_config)config;
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
	auto config_option = DBConfig::GetOptionByName(name);
	if (!config_option) {
		return DuckDBError;
	}
	try {
		auto db_config = (DBConfig *)config;
		db_config->SetOption(*config_option, Value(option));
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

duckdb_state duckdb_add_custom_buffer_manager(duckdb_config config, void *allocation_context,
                                              duckdb_allocate_func allocate_func,
                                              duckdb_reallocate_func reallocate_func, duckdb_destroy_func destroy_func,
                                              duckdb_buffer_allocation get_allocation_func, duckdb_pin_func pin_func,
                                              duckdb_unpin_func unpin_func, duckdb_max_memory_func max_memory_func,
                                              duckdb_used_memory_func used_memory_func) {

	// 'allocation_context' is allowed to be NULL, we don't dereference it anyways
	if (!config || !allocate_func || !reallocate_func || !destroy_func || !get_allocation_func || !pin_func ||
	    !unpin_func || !max_memory_func || !used_memory_func) {
		return DuckDBError;
	}
	auto db_config = (DBConfig *)config;

	duckdb::CBufferManagerConfig cbuffer_manager_config;
	cbuffer_manager_config.data = allocation_context;
	cbuffer_manager_config.allocate_func = allocate_func;
	cbuffer_manager_config.get_allocation_func = get_allocation_func;
	cbuffer_manager_config.reallocate_func = reallocate_func;
	cbuffer_manager_config.destroy_func = destroy_func;
	cbuffer_manager_config.pin_func = pin_func;
	cbuffer_manager_config.unpin_func = unpin_func;
	cbuffer_manager_config.max_memory_func = max_memory_func;
	cbuffer_manager_config.used_memory_func = used_memory_func;

	auto cbuffer_manager = make_unique<duckdb::CBufferManager>(cbuffer_manager_config);
	db_config->SetVirtualBufferManager(move(cbuffer_manager));
	return DuckDBSuccess;
}
