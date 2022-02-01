#include "json_common.hpp"

namespace duckdb {

//! Some defines copied from yyjson.cpp
#define IDX_T_SAFE_DIG 19
#define IDX_T_MAX      ((idx_t)(~(idx_t)0))

static inline idx_t ReadString(const char *ptr, const char *const end) {
	const char *const before = ptr;
	while (ptr != end) {
		if (*ptr == '.' || *ptr == '[' || *ptr) {
			break;
		}
		ptr++;
	}
	return ptr - before;
}

static inline idx_t ReadIndex(const char *ptr, const char *const end, idx_t &idx) {
	const char *const before = ptr;
	idx = 0;
	for (idx_t i = 0; i < IDX_T_SAFE_DIG; i++) {
		const auto &c = *ptr++;
		if (c == ']' || ptr == end) {
			break;
		}
		uint8_t add = (uint8_t)(c - '0');
		if (add <= 9) {
			idx = add + idx * 10;
		} else {
			// Not a digit
			return 0;
		}
	}
	// Invalid if last char was not ']', or if overflow
	if (*(ptr - 1) != ']' || idx >= (idx_t)IDX_T_MAX) {
		return 0;
	}
	return ptr - before;
}

bool JSONCommon::ValidPathDollar(const char *ptr, const idx_t &len) {
	const char *const end = ptr + len;
	// Skip past '$'
	ptr++;
	while (ptr != end) {
		const auto &c = *ptr++;
		if (c == '.') {
			// Object
			auto key_len = ReadString(ptr, end);
			if (key_len == 0) {
				return false;
			}
			ptr += key_len;
		} else if (c == '[') {
			// Array
			if (*ptr == '#') {
				// Index from back of array
				ptr++;
				if (*ptr == '-') {
					ptr++;
				} else if (*ptr == ']') {
					ptr++;
					continue;
				}
			}
			idx_t idx;
			auto idx_len = ReadIndex(ptr, end, idx);
			if (idx_len == 0) {
				return false;
			}
			ptr += idx_len;
		} else {
			return false;
		}
	}
	return true;
}

yyjson_val *JSONCommon::GetPointerDollar(yyjson_val *val, const char *ptr, const idx_t &len) {
	if (len == 1) {
		// Just '$'
		return val;
	}
	const char *const end = ptr + len;
	// Skip past '$'
	ptr++;
	while (val != nullptr && ptr != end) {
		const auto &c = *ptr++;
		if (c == '.') {
			// Object
			if (!yyjson_is_obj(val)) {
				return nullptr;
			}
			auto key_len = ReadString(ptr, end);
			val = yyjson_obj_getn(val, ptr, key_len);
			ptr += key_len;
		} else if (c == '[') {
			// Array
			if (!yyjson_is_arr(val)) {
				return nullptr;
			}
			bool from_back = false;
			if (*ptr == '#') {
				// Index from back of array
				ptr++;
				if (*ptr == ']') {
					return nullptr;
				}
				from_back = true;
				// Skip past '-'
				ptr++;
			}
			// Read index
			idx_t idx;
			auto idx_len = ReadIndex(ptr, end, idx);
			if (from_back) {
				auto arr_size = yyjson_arr_size(val);
				idx = idx > arr_size ? arr_size : arr_size - idx;
			}
			val = yyjson_arr_get(val, idx);
			ptr += idx_len;
		} else {
			throw InternalException("Unexpected char when parsing JSON path");
		}
	}
	return val;
}

} // namespace duckdb