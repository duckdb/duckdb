#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "utf8proc_wrapper.hpp"

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_validate_utf8(duckdb_v2_str text, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!text.ptr && text.len) {
			throw duckdb::InvalidInputException("UTF-8 byte range cannot be null unless it is empty");
		}
		if (text.len && duckdb::Utf8Proc::Analyze(text.ptr, text.len) == duckdb::UnicodeType::INVALID) {
			throw duckdb::InvalidInputException("Invalid UTF-8: VARCHAR must contain valid UTF-8 text");
		}
	});
}
