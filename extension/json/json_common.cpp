#include "json_common.hpp"

namespace duckdb {

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
