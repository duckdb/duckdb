#include "json_common.hpp"

namespace duckdb {

void JSONCommon::ThrowValFormatError(string error_string, yyjson_val *val) {
	JSONAllocator json_allocator(Allocator::DefaultAllocator());
	idx_t len;
	auto data = JSONCommon::WriteVal<yyjson_val>(val, json_allocator.GetYYJSONAllocator(), len);
	error_string = StringUtil::Format(error_string, string(data, len));
	throw InvalidInputException(error_string);
}

string ThrowPathError(const char *ptr, const char *end) {
	ptr--;
	throw InvalidInputException("JSON path error near '%s'", string(ptr, end - ptr));
}

void JSONCommon::ValidatePathDollar(const char *ptr, const idx_t &len) {
	const char *const end = ptr + len;
	// Skip past '$'
	ptr++;
	while (ptr != end) {
		const auto &c = *ptr++;
		if (c == '.') {
			// Object
			bool escaped = false;
			if (*ptr == '"') {
				// Skip past opening '"'
				ptr++;
				escaped = true;
			}
			auto key_len = ReadString(ptr, end, escaped);
			if (key_len == 0) {
				ThrowPathError(ptr, end);
			}
			ptr += key_len;
			if (escaped) {
				// Skip past closing '"'
				ptr++;
			}
		} else if (c == '[') {
			// Array
			if (*ptr == '#') {
				// Index from back of array
				ptr++;
				if (*ptr == ']') {
					ptr++;
					continue;
				}
				if (*ptr != '-') {
					ThrowPathError(ptr, end);
				}
				// Skip past '-'
				ptr++;
			}
			idx_t idx;
			auto idx_len = ReadIndex(ptr, end, idx);
			if (idx_len == 0) {
				ThrowPathError(ptr, end);
			}
			ptr += idx_len;
			// Skip past closing ']'
			ptr++;
		} else {
			ThrowPathError(ptr, end);
		}
	}
}

} // namespace duckdb
