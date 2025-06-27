#include "duckdb/main/capi/capi_internal.hpp"

using duckdb::ErrorData;
using duckdb::ErrorDataWrapper;

duckdb_error_data duckdb_create_error_data(duckdb_error_type type, const char *message) {
	auto wrapper = new ErrorDataWrapper();
	wrapper->error_data = ErrorData(duckdb::ErrorTypeFromC(type), message);
	return reinterpret_cast<duckdb_error_data>(wrapper);
}

void duckdb_destroy_error_data(duckdb_error_data *error_data) {
	if (!error_data || !*error_data) {
		return;
	}
	auto wrapper = reinterpret_cast<ErrorDataWrapper *>(*error_data);
	delete wrapper;
	*error_data = nullptr;
}

duckdb_error_type duckdb_error_data_error_type(duckdb_error_data error_data) {
	if (!error_data) {
		return DUCKDB_ERROR_INVALID_TYPE;
	}

	auto *wrapper = reinterpret_cast<ErrorDataWrapper *>(error_data);
	return duckdb::ErrorTypeToC(wrapper->error_data.Type());
}

const char *duckdb_error_data_message(duckdb_error_data error_data) {
	if (!error_data) {
		return nullptr;
	}

	auto *wrapper = reinterpret_cast<ErrorDataWrapper *>(error_data);
	return wrapper->error_data.RawMessage().c_str();
}

bool duckdb_error_data_has_error(duckdb_error_data error_data) {
	if (!error_data) {
		return false;
	}

	auto *wrapper = reinterpret_cast<ErrorDataWrapper *>(error_data);
	return wrapper->error_data.HasError();
}
