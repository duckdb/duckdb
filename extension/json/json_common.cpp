#include "json_common.hpp"

namespace duckdb {

using JSONPathType = JSONCommon::JSONPathType;

string JSONCommon::ValToString(yyjson_val *val, idx_t max_len) {
	JSONAllocator json_allocator(Allocator::DefaultAllocator());
	idx_t len;
	auto data = JSONCommon::WriteVal<yyjson_val>(val, json_allocator.GetYYAlc(), len);
	if (max_len < len) {
		return string(data, max_len) + "...";
	} else {
		return string(data, len);
	}
}

void JSONCommon::ThrowValFormatError(string error_string, yyjson_val *val) {
	error_string = StringUtil::Format(error_string, JSONCommon::ValToString(val));
	throw InvalidInputException(error_string);
}

string ThrowPathError(const char *ptr, const char *end, const bool binder) {
	ptr--;
	auto msg = StringUtil::Format("JSON path error near '%s'", string(ptr, end - ptr));
	if (binder) {
		throw BinderException(msg);
	} else {
		throw InvalidInputException(msg);
	}
}

static inline idx_t ReadString(const char *ptr, const char *const end, const bool escaped) {
	const char *const before = ptr;
	if (escaped) {
		while (ptr != end) {
			if (*ptr == '"') {
				break;
			}
			ptr++;
		}
		return ptr == end ? 0 : ptr - before;
	} else {
		while (ptr != end) {
			if (*ptr == '.' || *ptr == '[') {
				break;
			}
			ptr++;
		}
		return ptr - before;
	}
}

static inline idx_t ReadInteger(const char *ptr, const char *const end, idx_t &idx) {
	static constexpr auto IDX_T_SAFE_DIG = 19;
	static constexpr auto IDX_T_MAX = ((idx_t)(~(idx_t)0));

	const char *const before = ptr;
	idx = 0;
	for (idx_t i = 0; i < IDX_T_SAFE_DIG; i++) {
		if (ptr == end) {
			// No closing ']'
			return 0;
		}
		if (*ptr == ']') {
			break;
		}
		uint8_t add = (uint8_t)(*ptr - '0');
		if (add <= 9) {
			idx = add + idx * 10;
		} else {
			// Not a digit
			return 0;
		}
		ptr++;
	}
	// Invalid if overflow
	return idx >= (idx_t)IDX_T_MAX ? 0 : ptr - before;
}

static inline bool ReadKey(const char *&ptr, const char *const end, const char *&key_ptr, idx_t &key_len) {
	D_ASSERT(ptr != end);
	if (*ptr == '*') { // Wildcard
		ptr++;
		key_len = DConstants::INVALID_INDEX;
		return true;
	}
	bool escaped = false;
	if (*ptr == '"') {
		ptr++; // Skip past opening '"'
		escaped = true;
	}
	key_ptr = ptr;
	key_len = ReadString(ptr, end, escaped);
	if (key_len == 0) {
		return false;
	}
	ptr += key_len;
	if (escaped) {
		ptr++; // Skip past closing '"'
	}
	return true;
}

static inline bool ReadArrayIndex(const char *&ptr, const char *const end, idx_t &array_index, bool &from_back) {
	D_ASSERT(ptr != end);
	from_back = false;
	if (*ptr == '*') { // Wildcard
		ptr++;
		if (ptr == end || *ptr != ']') {
			return false;
		}
		array_index = DConstants::INVALID_INDEX;
	} else {
		if (*ptr == '#') { // SQLite syntax to index from back of array
			ptr++;         // Skip over '#'
			if (ptr == end) {
				return false;
			}
			if (*ptr == ']') {
				// [#] always returns NULL in SQLite, so we return an array index that will do the same
				array_index = NumericLimits<uint32_t>::Maximum();
				ptr++;
				return true;
			}
			if (*ptr != '-') {
				return false;
			}
			from_back = true;
		}
		if (*ptr == '-') {
			ptr++; // Skip over '-'
			from_back = true;
		}
		auto idx_len = ReadInteger(ptr, end, array_index);
		if (idx_len == 0) {
			return false;
		}
		ptr += idx_len;
	}
	ptr++; // Skip past closing ']'
	return true;
}

JSONPathType JSONCommon::ValidatePath(const char *ptr, const idx_t &len, const bool binder) {
	D_ASSERT(len >= 1 && *ptr == '$');
	JSONPathType path_type = JSONPathType::REGULAR;
	const char *const end = ptr + len;
	ptr++; // Skip past '$'
	while (ptr != end) {
		const auto &c = *ptr++;
		if (ptr == end) {
			ThrowPathError(ptr, end, binder);
		}
		switch (c) {
		case '.': { // Object field
			const char *key_ptr;
			idx_t key_len;
			if (!ReadKey(ptr, end, key_ptr, key_len)) {
				ThrowPathError(ptr, end, binder);
			}
			if (key_len == DConstants::INVALID_INDEX) {
				path_type = JSONPathType::WILDCARD;
			}
			break;
		}
		case '[': { // Array index
			idx_t array_index;
			bool from_back;
			if (!ReadArrayIndex(ptr, end, array_index, from_back)) {
				ThrowPathError(ptr, end, binder);
			}
			if (array_index == DConstants::INVALID_INDEX) {
				path_type = JSONPathType::WILDCARD;
			}
			break;
		}
		default:
			ThrowPathError(ptr, end, binder);
		}
	}
	return path_type;
}

yyjson_val *JSONCommon::GetPath(yyjson_val *val, const char *ptr, const idx_t &len) {
	// Path has been validated at this point
	const char *const end = ptr + len;
	ptr++; // Skip past '$'
	while (val != nullptr && ptr != end) {
		const auto &c = *ptr++;
		D_ASSERT(ptr != end);
		switch (c) {
		case '.': { // Object field
			if (!unsafe_yyjson_is_obj(val)) {
				return nullptr;
			}
			const char *key_ptr;
			idx_t key_len;
#ifdef DEBUG
			bool success =
#endif
			    ReadKey(ptr, end, key_ptr, key_len);
#ifdef DEBUG
			D_ASSERT(success);
#endif
			val = yyjson_obj_getn(val, key_ptr, key_len);
			break;
		}
		case '[': { // Array index
			if (!unsafe_yyjson_is_arr(val)) {
				return nullptr;
			}
			idx_t array_index;
			bool from_back;
#ifdef DEBUG
			bool success =
#endif
			    ReadArrayIndex(ptr, end, array_index, from_back);
#ifdef DEBUG
			D_ASSERT(success);
#endif
			if (from_back && array_index != 0) {
				array_index = unsafe_yyjson_get_len(val) - array_index;
			}
			val = yyjson_arr_get(val, array_index);
			break;
		}
		default: // LCOV_EXCL_START
			throw InternalException(
			    "Invalid JSON Path encountered in JSONCommon::GetPath, call JSONCommon::ValidatePath first!");
		} // LCOV_EXCL_STOP
	}
	return val;
}

void GetWildcardPathInternal(yyjson_val *val, const char *ptr, const char *const end, vector<yyjson_val *> &vals) {
	while (val != nullptr && ptr != end) {
		const auto &c = *ptr++;
		D_ASSERT(ptr != end);
		switch (c) {
		case '.': { // Object field
			if (!unsafe_yyjson_is_obj(val)) {
				return;
			}
			const char *key_ptr;
			idx_t key_len;
#ifdef DEBUG
			bool success =
#endif
			    ReadKey(ptr, end, key_ptr, key_len);
#ifdef DEBUG
			D_ASSERT(success);
#endif
			if (key_len == DConstants::INVALID_INDEX) { // Wildcard
				size_t idx, max;
				yyjson_val *key, *obj_val;
				yyjson_obj_foreach(val, idx, max, key, obj_val) {
					GetWildcardPathInternal(obj_val, ptr, end, vals);
				}
				return;
			}
			val = yyjson_obj_getn(val, key_ptr, key_len);
			break;
		}
		case '[': { // Array index
			if (!unsafe_yyjson_is_arr(val)) {
				return;
			}
			idx_t array_index;
			bool from_back;
#ifdef DEBUG
			bool success =
#endif
			    ReadArrayIndex(ptr, end, array_index, from_back);
#ifdef DEBUG
			D_ASSERT(success);
#endif

			if (array_index == DConstants::INVALID_INDEX) { // Wildcard
				size_t idx, max;
				yyjson_val *arr_val;
				yyjson_arr_foreach(val, idx, max, arr_val) {
					GetWildcardPathInternal(arr_val, ptr, end, vals);
				}
				return;
			}
			if (from_back && array_index != 0) {
				array_index = unsafe_yyjson_get_len(val) - array_index;
			}
			val = yyjson_arr_get(val, array_index);
			break;
		}
		default: // LCOV_EXCL_START
			throw InternalException(
			    "Invalid JSON Path encountered in GetWildcardPathInternal, call JSONCommon::ValidatePath first!");
		} // LCOV_EXCL_STOP
	}
	if (val != nullptr) {
		vals.emplace_back(val);
	}
}

void JSONCommon::GetWildcardPath(yyjson_val *val, const char *ptr, const idx_t &len, vector<yyjson_val *> &vals) {
	// Path has been validated at this point
	const char *const end = ptr + len;
	ptr++; // Skip past '$'
	GetWildcardPathInternal(val, ptr, end, vals);
}

} // namespace duckdb
