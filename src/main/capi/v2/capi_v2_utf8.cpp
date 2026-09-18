#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "utf8proc_wrapper.hpp"

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_validate_utf8(duckdb_v2_str text, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		auto bytes = Convert(text);
		if (!duckdb::Utf8Proc::IsValid(bytes.data(), bytes.size())) {
			throw duckdb::InvalidInputException("Input is not valid UTF-8");
		}
	});
}
