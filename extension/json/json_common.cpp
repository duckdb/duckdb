#include "json_common.hpp"

#include "duckdb/common/exception/binder_exception.hpp"

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

struct JSONKeyReadResult {
public:
	static inline JSONKeyReadResult Empty() {
		return {idx_t(0), false, string()};
	}

	static inline JSONKeyReadResult WildCard() {
		return {1, false, "*"};
	}

	static inline JSONKeyReadResult RecWildCard() {
		return {2, true, "*"};
	}

	static inline JSONKeyReadResult RecWildCardShortcut() {
		return {1, true, "*"};
	}

	inline bool IsValid() {
		return (chars_read != 0);
	}

	inline bool IsWildCard() {
		return key == "*";
	}

public:
	idx_t chars_read;
	bool recursive;
	string key;
};

static inline JSONKeyReadResult ReadString(const char *ptr, const char *const end, const bool escaped) {
	const char *const before = ptr;
	if (escaped) {
		auto key = make_unsafe_uniq_array_uninitialized<char>(end - ptr);
		idx_t key_len = 0;

		bool backslash = false;
		while (ptr != end) {
			if (backslash) {
				if (*ptr != '"' && *ptr != '\\') {
					key[key_len++] = '\\';
				}
				backslash = false;
			} else {
				if (*ptr == '"') {
					break;
				} else if (*ptr == '\\') {
					backslash = true;
					ptr++;
					continue;
				}
			}
			key[key_len++] = *ptr++;
		}
		if (ptr == end || backslash) {
			return JSONKeyReadResult::Empty();
		} else {
			return {idx_t(ptr - before), false, string(key.get(), key_len)};
		}
	} else {
		while (ptr != end) {
			if (*ptr == '.' || *ptr == '[') {
				break;
			}
			ptr++;
		}
		return {idx_t(ptr - before), false, string(before, ptr - before)};
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

static inline JSONKeyReadResult ReadKey(const char *ptr, const char *const end) {
	D_ASSERT(ptr != end);
	if (*ptr == '*') { // Wildcard
		if (*(ptr + 1) == '*') {
			return JSONKeyReadResult::RecWildCard();
		}
		return JSONKeyReadResult::WildCard();
	}
	bool recursive = false;
	if (*ptr == '.') {
		char next = *(ptr + 1);
		if (next == '*') {
			return JSONKeyReadResult::RecWildCard();
		}
		if (next == '[') {
			return JSONKeyReadResult::RecWildCardShortcut();
		}
		ptr++;
		recursive = true;
	}
	bool escaped = false;
	if (*ptr == '"') {
		ptr++; // Skip past opening '"'
		escaped = true;
	}
	auto result = ReadString(ptr, end, escaped);
	if (!result.IsValid()) {
		return result;
	}
	if (escaped) {
		result.chars_read += 2; // Account for surrounding quotes
	}
	if (recursive) {
		result.chars_read += 1;
		result.recursive = true;
	}
	return result;
}

static inline bool ReadArrayIndex(const char *&ptr, const char *const end, JSONPathElement &element) {
	D_ASSERT(ptr != end);
	if (*ptr == '*') { // Wildcard
		ptr++;
		if (ptr == end || *ptr != ']') {
			return false;
		}
		element.type = JSONPathElementType::WILDCARD;
	} else {
		element.type = JSONPathElementType::INDEX;
		if (*ptr == '#') { // SQLite syntax to index from back of array
			ptr++;         // Skip over '#'
			if (ptr == end) {
				return false;
			}
			if (*ptr == ']') {
				element.type = JSONPathElementType::APPEND;
				ptr++;
				return true;
			}
			if (*ptr != '-') {
				return false;
			}
		}
		if (*ptr == '-') {
			ptr++; // Skip over '-'
			element.type = JSONPathElementType::REVERSE_INDEX;
		}
		auto idx_len = ReadInteger(ptr, end, element.index);
		if (idx_len == 0) {
			return false;
		}
		ptr += idx_len;
	}
	ptr++; // Skip past closing ']'
	return true;
}

JSONPathIterator::JSONPathIterator(const char *ptr_p, idx_t len, bool binder_p)
    : ptr(ptr_p), end(ptr_p + len), binder(binder_p) {
	D_ASSERT(len >= 1 && *ptr == '$');
	ptr++; // Skip past '$'
}

bool JSONPathIterator::Next(JSONPathElement &element) {
	if (ptr == end) {
		return false;
	}
	const auto &c = *ptr++;
	if (ptr == end) {
		ThrowPathError(ptr, end, binder);
	}
	switch (c) {
	case '.': { // Object field
		auto key = ReadKey(ptr, end);
		if (!key.IsValid()) {
			ThrowPathError(ptr, end, binder);
		}
		ptr += key.chars_read;
		if (key.recursive) {
			element.type = JSONPathElementType::RECURSIVE_WILDCARD;
		} else if (key.IsWildCard()) {
			element.type = JSONPathElementType::WILDCARD;
		} else {
			element.type = JSONPathElementType::KEY;
			element.key = std::move(key.key);
		}
		break;
	}
	case '[': { // Array index
		if (!ReadArrayIndex(ptr, end, element)) {
			ThrowPathError(ptr, end, binder);
		}
		break;
	}
	default:
		ThrowPathError(ptr, end, binder);
	}
	return true;
}

JSONPathType JSONCommon::ValidatePath(const char *ptr, const idx_t &len, const bool binder) {
	D_ASSERT(len >= 1 && *ptr == '$');
	JSONPathType path_type = JSONPathType::REGULAR;
	JSONPathIterator iterator(ptr, len, binder);
	JSONPathElement element;
	while (iterator.Next(element)) {
		if (element.type == JSONPathElementType::WILDCARD || element.type == JSONPathElementType::RECURSIVE_WILDCARD) {
			path_type = JSONPathType::WILDCARD;
		}
	}
	return path_type;
}

//! Resolve a single path element against a JSON value
static inline yyjson_val *GetPathElement(yyjson_val *val, const JSONPathElement &element) {
	switch (element.type) {
	case JSONPathElementType::KEY:
		if (!unsafe_yyjson_is_obj(val)) {
			return nullptr;
		}
		return yyjson_obj_getn(val, element.key.c_str(), element.key.size());
	case JSONPathElementType::INDEX:
	case JSONPathElementType::REVERSE_INDEX: {
		if (!unsafe_yyjson_is_arr(val)) {
			return nullptr;
		}
		auto array_index = element.index;
		if (element.type == JSONPathElementType::REVERSE_INDEX && array_index != 0) {
			array_index = unsafe_yyjson_get_len(val) - array_index;
		}
		return yyjson_arr_get(val, array_index);
	}
	case JSONPathElementType::APPEND:
		// [#] always returns NULL in SQLite
		return nullptr;
	default: // LCOV_EXCL_START
		throw InternalException("Invalid JSON path element encountered, call JSONCommon::ValidatePath first!");
	} // LCOV_EXCL_STOP
}

yyjson_val *JSONCommon::GetPath(yyjson_val *val, const char *ptr, const idx_t &len) {
	// Path has been validated at this point
	JSONPathIterator iterator(ptr, len, false);
	JSONPathElement element;
	while (val != nullptr && iterator.Next(element)) {
		val = GetPathElement(val, element);
	}
	return val;
}

vector<JSONPathElement> JSONCommon::ParsePathElements(const char *ptr, idx_t len, bool binder) {
	vector<JSONPathElement> elements;
	JSONPathIterator iterator(ptr, len, binder);
	JSONPathElement element;
	while (iterator.Next(element)) {
		elements.push_back(element);
	}
	return elements;
}

yyjson_val *JSONCommon::GetPathElements(yyjson_val *val, const vector<JSONPathElement> &elements) {
	for (idx_t i = 0; val != nullptr && i < elements.size(); i++) {
		val = GetPathElement(val, elements[i]);
	}
	return val;
}

void GetWildcardPathInternal(yyjson_val *val, const char *ptr, const char *const end, vector<yyjson_val *> &vals) {
	while (val != nullptr && ptr != end) {
		const auto &c = *ptr++;
		D_ASSERT(ptr != end);
		switch (c) {
		case '.': { // Object field
			auto key_result = ReadKey(ptr, end);
			D_ASSERT(key_result.IsValid());
			if (key_result.recursive) {
				if (key_result.IsWildCard()) {
					ptr += key_result.chars_read;
				}
				vector<yyjson_val *> rec_vals;
				rec_vals.emplace_back(val);
				for (idx_t i = 0; i < rec_vals.size(); i++) {
					yyjson_val *rec_val = rec_vals[i];
					if (yyjson_is_arr(rec_val)) {
						size_t idx, max;
						yyjson_val *element;
						yyjson_arr_foreach(rec_val, idx, max, element) {
							rec_vals.emplace_back(element);
						}
					} else if (yyjson_is_obj(rec_val)) {
						size_t idx, max;
						yyjson_val *key, *element;
						yyjson_obj_foreach(rec_val, idx, max, key, element) {
							rec_vals.emplace_back(element);
						}
					}
					if (i > 0 || ptr != end) {
						GetWildcardPathInternal(rec_val, ptr, end, vals);
					}
				}
				return;
			}
			ptr += key_result.chars_read;
			if (!unsafe_yyjson_is_obj(val)) {
				return;
			}
			if (key_result.IsWildCard()) { // Wildcard
				size_t idx, max;
				yyjson_val *key, *obj_val;
				yyjson_obj_foreach(val, idx, max, key, obj_val) {
					GetWildcardPathInternal(obj_val, ptr, end, vals);
				}
				return;
			}
			val = yyjson_obj_getn(val, key_result.key.c_str(), key_result.key.size());
			break;
		}
		case '[': { // Array index
			if (!unsafe_yyjson_is_arr(val)) {
				return;
			}
			JSONPathElement element;
#ifdef DEBUG
			bool success =
#endif
			    ReadArrayIndex(ptr, end, element);
#ifdef DEBUG
			D_ASSERT(success);
#endif

			if (element.type == JSONPathElementType::WILDCARD) {
				size_t idx, max;
				yyjson_val *arr_val;
				yyjson_arr_foreach(val, idx, max, arr_val) {
					GetWildcardPathInternal(arr_val, ptr, end, vals);
				}
				return;
			}
			if (element.type == JSONPathElementType::APPEND) {
				// [#] always returns NULL in SQLite
				return;
			}
			auto array_index = element.index;
			if (element.type == JSONPathElementType::REVERSE_INDEX && array_index != 0) {
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
	return;
}

void JSONCommon::GetWildcardPath(yyjson_val *val, const char *ptr, const idx_t &len, vector<yyjson_val *> &vals) {
	// Path has been validated at this point
	const char *const end = ptr + len;
	ptr++; // Skip past '$'
	GetWildcardPathInternal(val, ptr, end, vals);
}

} // namespace duckdb
