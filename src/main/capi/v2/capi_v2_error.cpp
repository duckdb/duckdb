#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_error_info_get_text(duckdb_v2_error_info_handle info, duckdb_v2_str *out_text) {
	if (!info || !out_text) {
		return DUCKDB_V2_ERROR_INPUT_INVALID;
	}
	const auto *ei = Convert(info);
	*out_text = Convert(ei->message);
	return DUCKDB_V2_ERROR_NONE;
}

DUCKDB_V2_ERROR duckdb_v2_error_info_get_raw_message(duckdb_v2_error_info_handle info, duckdb_v2_str *out_raw_message) {
	if (!info || !out_raw_message) {
		return DUCKDB_V2_ERROR_INPUT_INVALID;
	}
	const auto *ei = Convert(info);
	*out_raw_message = ei->raw_message.empty() ? duckdb_v2_str {nullptr, 0} : Convert(ei->raw_message);
	return DUCKDB_V2_ERROR_NONE;
}

DUCKDB_V2_ERROR duckdb_v2_error_info_set_text(duckdb_v2_error_info_handle info, duckdb_v2_str text) {
	if (!info || (!text.ptr && text.len > 0)) {
		return DUCKDB_V2_ERROR_INPUT_INVALID;
	}
	auto *ei = Convert(info);
	ei->message = Convert(text);
	ei->raw_message.clear(); // Directly-set message has no raw body.
	return DUCKDB_V2_ERROR_NONE;
}

DUCKDB_V2_ERROR duckdb_v2_error_info_get_code(duckdb_v2_error_info_handle info, DUCKDB_V2_ERROR *out_code) {
	if (!info || !out_code) {
		return DUCKDB_V2_ERROR_INPUT_INVALID;
	}
	const auto *ei = Convert(info);
	*out_code = ei->code;
	return DUCKDB_V2_ERROR_NONE;
}

DUCKDB_V2_ERROR duckdb_v2_error_info_set_code(duckdb_v2_error_info_handle info, DUCKDB_V2_ERROR code) {
	if (!info) {
		return DUCKDB_V2_ERROR_INPUT_INVALID;
	}
	const auto ei = Convert(info);
	ei->code = code;
	return DUCKDB_V2_ERROR_NONE;
}

DUCKDB_V2_ERROR duckdb_v2_error_info_destroy(duckdb_v2_error_info_handle *info) {
	if (info && *info) {
		delete Convert(*info);
		*info = nullptr;
	}
	return DUCKDB_V2_ERROR_NONE;
}
