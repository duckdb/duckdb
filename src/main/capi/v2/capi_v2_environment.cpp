#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_create_environment(duckdb_v2_environment_handle *out_env, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!out_env) {
			throw duckdb::InvalidInputException("null out_env in duckdb_v2_create_environment");
		}
		*out_env = nullptr;
		auto wrapper = duckdb::make_uniq<CV2Environment>();
		wrapper->cache = duckdb::make_uniq<duckdb::DBInstanceCache>();
		*out_env = Convert(wrapper.release());
	});
}

// destroy_environment keeps a manual return path so the open-databases case
// can surface as RESOURCE_IN_USE — there is no ExceptionType that maps to
// that V2 code, so routing it through WithErrorHandler would degrade it.
DUCKDB_V2_ERROR duckdb_v2_destroy_environment(duckdb_v2_environment_handle *env) {
	if (!env || !*env) {
		return DUCKDB_V2_ERROR_NONE;
	}
	const auto *wrapper = Convert(*env);
	auto count = wrapper->open_database_count.load(std::memory_order_acquire);
	if (count != 0) {
		return DUCKDB_V2_ERROR_RESOURCE_IN_USE;
	}
	delete wrapper;
	*env = nullptr;
	return DUCKDB_V2_ERROR_NONE;
}

DUCKDB_V2_ERROR duckdb_v2_environment_database_count(duckdb_v2_environment_handle env, idx_t *out_count,
                                                     duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!env || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_environment_database_count");
		}
		*out_count = Convert(env)->open_database_count.load();
	});
}
