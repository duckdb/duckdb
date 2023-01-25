//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "yyjson.hpp"

namespace duckdb {

class JSONAllocator {
public:
	explicit JSONAllocator(Allocator &allocator)
	    : arena_allocator(allocator), yyjson_allocator({Allocate, Reallocate, Free, &arena_allocator}) {
	}

	inline yyjson_alc *GetYYJSONAllocator() {
		return &yyjson_allocator;
	}

	void Reset() {
		arena_allocator.Reset();
	}

private:
	static inline void *Allocate(void *ctx, size_t size) {
		auto alloc = (ArenaAllocator *)ctx;
		return alloc->AllocateAligned(size);
	}

	static inline void *Reallocate(void *ctx, void *ptr, size_t old_size, size_t size) {
		auto alloc = (ArenaAllocator *)ctx;
		return alloc->ReallocateAligned((data_ptr_t)ptr, old_size, size);
	}

	static inline void Free(void *ctx, void *ptr) {
		// NOP because ArenaAllocator can't free
	}

private:
	ArenaAllocator arena_allocator;
	yyjson_alc yyjson_allocator;
};

struct JSONCommon {
public:
	static constexpr auto JSON_TYPE_NAME = "JSON";

	static const LogicalType JSONType() {
		auto json_type = LogicalType(LogicalTypeId::VARCHAR);
		json_type.SetAlias(JSON_TYPE_NAME);
		return json_type;
	}

	static bool LogicalTypeIsJSON(const LogicalType &type) {
		return type.id() == LogicalTypeId::VARCHAR && type.HasAlias() && type.GetAlias() == JSON_TYPE_NAME;
	}

public:
	//! Read/Write flags
	static constexpr auto READ_FLAG = YYJSON_READ_ALLOW_INF_AND_NAN | YYJSON_READ_ALLOW_TRAILING_COMMAS;
	static constexpr auto STOP_READ_FLAG = READ_FLAG | YYJSON_READ_STOP_WHEN_DONE | YYJSON_READ_INSITU;
	static constexpr auto WRITE_FLAG = YYJSON_WRITE_ALLOW_INF_AND_NAN;

public:
	//! Constant JSON type strings
	static constexpr char const *TYPE_STRING_NULL = "NULL";
	static constexpr char const *TYPE_STRING_BOOLEAN = "BOOLEAN";
	static constexpr char const *TYPE_STRING_BIGINT = "BIGINT";
	static constexpr char const *TYPE_STRING_UBIGINT = "UBIGINT";
	static constexpr char const *TYPE_STRING_DOUBLE = "DOUBLE";
	static constexpr char const *TYPE_STRING_VARCHAR = "VARCHAR";
	static constexpr char const *TYPE_STRING_ARRAY = "ARRAY";
	static constexpr char const *TYPE_STRING_OBJECT = "OBJECT";

	template <class YYJSON_VAL_T>
	static inline const char *const ValTypeToString(YYJSON_VAL_T *val) {
		switch (GetTag<YYJSON_VAL_T>(val)) {
		case YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE:
			return JSONCommon::TYPE_STRING_NULL;
		case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
			return JSONCommon::TYPE_STRING_VARCHAR;
		case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
			return JSONCommon::TYPE_STRING_ARRAY;
		case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
			return JSONCommon::TYPE_STRING_OBJECT;
		case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_TRUE:
		case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_FALSE:
			return JSONCommon::TYPE_STRING_BOOLEAN;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
			return JSONCommon::TYPE_STRING_UBIGINT;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
			return JSONCommon::TYPE_STRING_BIGINT;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
			return JSONCommon::TYPE_STRING_DOUBLE;
		default:
			throw InternalException("Unexpected yyjson tag in ValTypeToString");
		}
	}

	template <class YYJSON_VAL_T>
	static inline constexpr string_t ValTypeToStringT(YYJSON_VAL_T *val) {
		return string_t(ValTypeToString<YYJSON_VAL_T>(val));
	}

public:
	static inline yyjson_mut_doc *CreateDocument(yyjson_alc *alc) {
		D_ASSERT(alc);
		return yyjson_mut_doc_new(alc);
	}
	static inline yyjson_doc *ReadDocumentUnsafe(char *data, idx_t size, const yyjson_read_flag flg, yyjson_alc *alc,
	                                             yyjson_read_err *err = nullptr) {
		D_ASSERT(alc);
		return yyjson_read_opts(data, size, flg, alc, err);
	}
	static inline yyjson_doc *ReadDocumentUnsafe(const string_t &input, const yyjson_read_flag flg, yyjson_alc *alc,
	                                             yyjson_read_err *err = nullptr) {
		return ReadDocumentUnsafe(input.GetDataWriteable(), input.GetSize(), flg, alc, err);
	}
	static inline yyjson_doc *ReadDocument(char *data, idx_t size, const yyjson_read_flag flg, yyjson_alc *alc) {
		yyjson_read_err error;
		auto result = ReadDocumentUnsafe(data, size, flg, alc, &error);
		if (error.code != YYJSON_READ_SUCCESS) {
			ThrowParseError(data, size, error);
		}
		return result;
	}
	static inline yyjson_doc *ReadDocument(const string_t &input, const yyjson_read_flag flg, yyjson_alc *alc) {
		return ReadDocument(input.GetDataWriteable(), input.GetSize(), flg, alc);
	}
	static string FormatParseError(const char *data, idx_t length, yyjson_read_err &error, const string &extra = "") {
		D_ASSERT(error.code != YYJSON_READ_SUCCESS);
		// Truncate, so we don't print megabytes worth of JSON
		string input = length > 50 ? string(data, 47) + "..." : string(data, length);
		// Have to replace \r, otherwise output is unreadable
		input = StringUtil::Replace(input, "\r", "\\r");
		return StringUtil::Format("Malformed JSON at byte %lld of input: %s. %s Input: %s", error.pos, error.msg, extra,
		                          input);
	}
	static void ThrowParseError(const char *data, idx_t length, yyjson_read_err &error, const string &extra = "") {
		throw InvalidInputException(FormatParseError(data, length, error, extra));
	}

	template <class YYJSON_VAL_T>
	static inline char *WriteVal(YYJSON_VAL_T *val, yyjson_alc *alc, idx_t &len) {
		throw InternalException("Unknown yyjson val type");
	}
	template <class YYJSON_VAL_T>
	static inline string_t WriteVal(YYJSON_VAL_T *val, yyjson_alc *alc) {
		D_ASSERT(alc);
		idx_t len;
		auto data = WriteVal<YYJSON_VAL_T>(val, alc, len);
		return string_t(data, len);
	}
	//! Throw an error with the printed yyjson_val
	static void ThrowValFormatError(string error_string, yyjson_val *val);

public:
	//! Validate path with $ syntax
	static void ValidatePathDollar(const char *ptr, const idx_t &len);

	//! Get JSON value using JSON path query (safe, checks the path query)
	template <class YYJSON_VAL_T>
	static inline YYJSON_VAL_T *GetPointer(YYJSON_VAL_T *root, const string_t &path_str) {
		auto ptr = path_str.GetDataUnsafe();
		auto len = path_str.GetSize();
		if (len == 0) {
			return GetPointerUnsafe<YYJSON_VAL_T>(root, ptr, len);
		}
		switch (*ptr) {
		case '/': {
			// '/' notation must be '\0'-terminated
			auto str = string(ptr, len);
			return GetPointerUnsafe<YYJSON_VAL_T>(root, str.c_str(), len);
		}
		case '$': {
			ValidatePathDollar(ptr, len);
			return GetPointerUnsafe<YYJSON_VAL_T>(root, ptr, len);
		}
		default:
			auto str = "/" + string(ptr, len);
			return GetPointerUnsafe<YYJSON_VAL_T>(root, str.c_str(), len + 1);
		}
	}

	//! Get JSON value using JSON path query (unsafe)
	template <class YYJSON_VAL_T>
	static inline YYJSON_VAL_T *GetPointerUnsafe(YYJSON_VAL_T *root, const char *ptr, const idx_t &len) {
		if (len == 0) {
			return nullptr;
		}
		switch (*ptr) {
		case '/':
			return TemplatedGetPointer<YYJSON_VAL_T>(root, ptr, len);
		case '$':
			return TemplatedGetPointerDollar<YYJSON_VAL_T>(root, ptr, len);
		default:
			throw InternalException("JSON path does not start with '/' or '$'");
		}
	}

private:
	//! Get JSON pointer using /field/index/... notation
	template <class YYJSON_VAL_T>
	static inline YYJSON_VAL_T *TemplatedGetPointer(YYJSON_VAL_T *root, const char *ptr, const idx_t &len) {
		throw InternalException("Unknown yyjson value type");
	}

	//! Get JSON pointer using $.field[index]... notation
	template <class YYJSON_VAL_T>
	static YYJSON_VAL_T *TemplatedGetPointerDollar(YYJSON_VAL_T *val, const char *ptr, const idx_t &len) {
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
				if (!IsObj<YYJSON_VAL_T>(val)) {
					return nullptr;
				}
				bool escaped = false;
				if (*ptr == '"') {
					// Skip past opening '"'
					ptr++;
					escaped = true;
				}
				auto key_len = ReadString(ptr, end, escaped);
				val = ObjGetN<YYJSON_VAL_T>(val, ptr, key_len);
				ptr += key_len;
				if (escaped) {
					// Skip past closing '"'
					ptr++;
				}
			} else if (c == '[') {
				// Array
				if (!IsArr<YYJSON_VAL_T>(val)) {
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
					auto arr_size = ArrSize<YYJSON_VAL_T>(val);
					idx = idx > arr_size ? arr_size : arr_size - idx;
				}
				val = ArrGet<YYJSON_VAL_T>(val, idx);
				ptr += idx_len;
				// Skip past closing ']'
				ptr++;
			} else {
				throw InternalException("Unexpected char when parsing JSON path");
			}
		}
		return val;
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

	static constexpr auto IDX_T_SAFE_DIG = 19;
	static constexpr auto IDX_T_MAX = ((idx_t)(~(idx_t)0));

	static inline idx_t ReadIndex(const char *ptr, const char *const end, idx_t &idx) {
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

private:
	template <class YYJSON_VAL_T>
	static inline bool IsObj(YYJSON_VAL_T *val) {
		throw InternalException("Unknown yyjson value type");
	}

	template <class YYJSON_VAL_T>
	static inline YYJSON_VAL_T *ObjGetN(YYJSON_VAL_T *val, const char *ptr, idx_t key_len) {
		throw InternalException("Unknown yyjson value type");
	}

	template <class YYJSON_VAL_T>
	static inline bool IsArr(YYJSON_VAL_T *val) {
		throw InternalException("Unknown yyjson value type");
	}

	template <class YYJSON_VAL_T>
	static inline size_t ArrSize(YYJSON_VAL_T *val) {
		throw InternalException("Unknown yyjson value type");
	}

	template <class YYJSON_VAL_T>
	static inline YYJSON_VAL_T *ArrGet(YYJSON_VAL_T *val, idx_t index) {
		throw InternalException("Unknown yyjson value type");
	}

	template <class YYJSON_VAL_T>
	static inline yyjson_type GetTag(YYJSON_VAL_T *val) {
		throw InternalException("Unknown yyjson value type");
	}
};

template <>
inline char *JSONCommon::WriteVal(yyjson_val *val, yyjson_alc *alc, idx_t &len) {
	return yyjson_val_write_opts(val, JSONCommon::WRITE_FLAG, alc, (size_t *)&len, nullptr);
}
template <>
inline char *JSONCommon::WriteVal(yyjson_mut_val *val, yyjson_alc *alc, idx_t &len) {
	return yyjson_mut_val_write_opts(val, JSONCommon::WRITE_FLAG, alc, (size_t *)&len, nullptr);
}

template <>
inline yyjson_val *JSONCommon::TemplatedGetPointer(yyjson_val *root, const char *ptr, const idx_t &len) {
	return len == 1 ? root : unsafe_yyjson_get_pointer(root, ptr, len);
}
template <>
inline yyjson_mut_val *JSONCommon::TemplatedGetPointer(yyjson_mut_val *root, const char *ptr, const idx_t &len) {
	return len == 1 ? root : unsafe_yyjson_mut_get_pointer(root, ptr, len);
}

template <>
inline bool JSONCommon::IsObj(yyjson_val *val) {
	return yyjson_is_obj(val);
}
template <>
inline bool JSONCommon::IsObj(yyjson_mut_val *val) {
	return yyjson_mut_is_obj(val);
}

template <>
inline yyjson_val *JSONCommon::ObjGetN(yyjson_val *val, const char *ptr, idx_t key_len) {
	return yyjson_obj_getn(val, ptr, key_len);
}
template <>
inline yyjson_mut_val *JSONCommon::ObjGetN(yyjson_mut_val *val, const char *ptr, idx_t key_len) {
	return yyjson_mut_obj_getn(val, ptr, key_len);
}

template <>
inline bool JSONCommon::IsArr(yyjson_val *val) {
	return yyjson_is_arr(val);
}
template <>
inline bool JSONCommon::IsArr(yyjson_mut_val *val) {
	return yyjson_mut_is_arr(val);
}

template <>
inline size_t JSONCommon::ArrSize(yyjson_val *val) {
	return yyjson_arr_size(val);
}
template <>
inline size_t JSONCommon::ArrSize(yyjson_mut_val *val) {
	return yyjson_mut_arr_size(val);
}

template <>
inline yyjson_val *JSONCommon::ArrGet(yyjson_val *val, idx_t index) {
	return yyjson_arr_get(val, index);
}
template <>
inline yyjson_mut_val *JSONCommon::ArrGet(yyjson_mut_val *val, idx_t index) {
	return yyjson_mut_arr_get(val, index);
}

template <>
inline yyjson_type JSONCommon::GetTag(yyjson_val *val) {
	return yyjson_get_tag(val);
}
template <>
inline yyjson_type JSONCommon::GetTag(yyjson_mut_val *val) {
	return yyjson_mut_get_tag(val);
}

} // namespace duckdb
