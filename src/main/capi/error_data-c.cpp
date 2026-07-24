#include "duckdb/main/capi/capi_internal.hpp"

using duckdb::ErrorData;
using duckdb::ErrorDataWrapper;
using duckdb::idx_t;

static const char *GetExtraInfoByIndex(duckdb_error_data error_data, idx_t index, bool return_key) {
	if (!error_data) {
		return nullptr;
	}
	auto *wrapper = reinterpret_cast<ErrorDataWrapper *>(error_data);
	idx_t current = 0;
	for (auto &entry : wrapper->error_data.ExtraInfo()) {
		if (current == index) {
			return return_key ? entry.first.c_str() : entry.second.c_str();
		}
		current++;
	}
	return nullptr;
}

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

idx_t duckdb_error_data_extra_info_count(duckdb_error_data error_data) {
	if (!error_data) {
		return 0;
	}
	auto *wrapper = reinterpret_cast<ErrorDataWrapper *>(error_data);
	return duckdb::NumericCast<idx_t>(wrapper->error_data.ExtraInfo().size());
}

const char *duckdb_error_data_extra_info_key(duckdb_error_data error_data, idx_t index) {
	return GetExtraInfoByIndex(error_data, index, true);
}

const char *duckdb_error_data_extra_info_value(duckdb_error_data error_data, idx_t index) {
	return GetExtraInfoByIndex(error_data, index, false);
}

const char *duckdb_error_data_extra_info_get(duckdb_error_data error_data, const char *key) {
	if (!error_data || !key) {
		return nullptr;
	}
	auto *wrapper = reinterpret_cast<ErrorDataWrapper *>(error_data);
	auto &extra_info = wrapper->error_data.ExtraInfo();
	auto entry = extra_info.find(key);
	if (entry == extra_info.end()) {
		return nullptr;
	}
	return entry->second.c_str();
}
