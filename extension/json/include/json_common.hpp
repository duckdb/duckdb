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
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "yyjson.hpp"

namespace duckdb {

struct JSONReadFunctionData : public FunctionData {
public:
	JSONReadFunctionData(bool constant, string path_p, idx_t len);
	unique_ptr<FunctionData> Copy() override;
	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments);

public:
	const bool constant;
	const string path;
	const char *ptr;
	const size_t len;
};

struct JSONReadManyFunctionData : public FunctionData {
public:
	JSONReadManyFunctionData(vector<string> paths_p, vector<size_t> lens_p);
	unique_ptr<FunctionData> Copy() override;
	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments);

public:
	const vector<string> paths;
	vector<const char *> ptrs;
	const vector<size_t> lens;
};

struct JSONCommon {
private:
	//! Read/Write flag that make sense for us
	static constexpr auto READ_FLAG = YYJSON_READ_ALLOW_INF_AND_NAN;
	static constexpr auto WRITE_FLAG = YYJSON_WRITE_ALLOW_INF_AND_NAN;

	//! Some defines copied from yyjson.cpp
	static constexpr auto IDX_T_SAFE_DIG = 19;
	static constexpr auto IDX_T_MAX = ((idx_t)(~(idx_t)0));

public:
	//! Constant JSON type strings
	static constexpr auto TYPE_STRING_NULL = "NULL";
	static constexpr auto TYPE_STRING_BOOLEAN = "BOOLEAN";
	static constexpr auto TYPE_STRING_BIGINT = "BIGINT";
	static constexpr auto TYPE_STRING_UBIGINT = "UBIGINT";
	static constexpr auto TYPE_STRING_DOUBLE = "DOUBLE";
	static constexpr auto TYPE_STRING_VARCHAR = "VARCHAR";
	static constexpr auto TYPE_STRING_ARRAY = "ARRAY";
	static constexpr auto TYPE_STRING_OBJECT = "OBJECT";

public:
	//! Read JSON document (returns nullptr if invalid JSON)
	static inline yyjson_doc *ReadDocumentUnsafe(const string_t &input) {
		return yyjson_read(input.GetDataUnsafe(), input.GetSize(), READ_FLAG);
	}
	//! Read JSON document (throws error if malformed JSON)
	static inline yyjson_doc *ReadDocument(const string_t &input) {
		auto result = ReadDocumentUnsafe(input);
		if (!result) {
			throw InvalidInputException("malformed JSON");
		}
		return result;
	}
	//! Write JSON value to string_t
	static inline string_t WriteVal(yyjson_mut_doc *doc, Vector &vector) {
		idx_t len;
		auto data = yyjson_mut_write(doc, WRITE_FLAG, (size_t *)&len);
		auto result = StringVector::AddString(vector, data, len);
		free(data);
		return result;
	}
	static inline string_t WriteVal(yyjson_val *val, Vector &vector) {
		// Create mutable copy of the read val
		auto *mut_doc = yyjson_mut_doc_new(nullptr);
		auto *mut_val = yyjson_val_mut_copy(mut_doc, val);
		yyjson_mut_doc_set_root(mut_doc, mut_val);
		auto result = WriteVal(mut_doc, vector);
		yyjson_mut_doc_free(mut_doc);
		return result;
	}
	//! Write the string if it's a string, else write the value
	static inline string_t WriteStringVal(yyjson_val *val, Vector &vector) {
		// Create mutable copy of the read val
		auto *mut_doc = yyjson_mut_doc_new(nullptr);
		auto *mut_val = yyjson_val_mut_copy(mut_doc, val);
		yyjson_mut_doc_set_root(mut_doc, mut_val);
		// Write mutable copy to string
		idx_t len;
		char *data = yyjson_mut_write(mut_doc, WRITE_FLAG, (size_t *)&len);
		// Remove quotes if necessary
		string_t result {};
		if (yyjson_is_str(val)) {
			result = StringVector::AddString(vector, data + 1, len - 2);
		} else {
			result = StringVector::AddString(vector, data, len);
		}
		free(data);
		yyjson_mut_doc_free(mut_doc);
		return result;
	}

	//! Get type string corresponding to yyjson type
	static inline const char *const ValTypeToString(yyjson_val *val) {
		switch (yyjson_get_type(val)) {
		case YYJSON_TYPE_NULL:
			return JSONCommon::TYPE_STRING_NULL;
		case YYJSON_TYPE_BOOL:
			return JSONCommon::TYPE_STRING_BOOLEAN;
		case YYJSON_TYPE_NUM:
			switch (unsafe_yyjson_get_subtype(val)) {
			case YYJSON_SUBTYPE_UINT:
				return JSONCommon::TYPE_STRING_UBIGINT;
			case YYJSON_SUBTYPE_SINT:
				return JSONCommon::TYPE_STRING_BIGINT;
			case YYJSON_SUBTYPE_REAL:
				return JSONCommon::TYPE_STRING_DOUBLE;
			default:
				throw InternalException("Unexpected yyjson subtype in ValTypeToString");
			}
		case YYJSON_TYPE_STR:
			return JSONCommon::TYPE_STRING_VARCHAR;
		case YYJSON_TYPE_ARR:
			return JSONCommon::TYPE_STRING_ARRAY;
		case YYJSON_TYPE_OBJ:
			return JSONCommon::TYPE_STRING_OBJECT;
		default:
			throw InternalException("Unexpected yyjson type in ValTypeToString");
		}
	}

public:
	//! Validate path with $ syntax
	static void ValidatePathDollar(const char *ptr, const idx_t &len);

	//! Get JSON value using JSON path query (safe, checks the path query)
	template <class yyjson_t>
	static inline yyjson_t *GetPointer(yyjson_t *root, const string_t &path_str) {
		auto ptr = path_str.GetDataUnsafe();
		auto len = path_str.GetSize();
		if (len == 0) {
			return GetPointerUnsafe<yyjson_t>(root, ptr, len);
		}
		switch (*ptr) {
		case '/': {
			// '/' notation must be '\0'-terminated
			auto str = string(ptr, len);
			return GetPointerUnsafe<yyjson_t>(root, str.c_str(), len);
		}
		case '$': {
			ValidatePathDollar(ptr, len);
			return GetPointerUnsafe<yyjson_t>(root, ptr, len);
		}
		default:
			auto str = "/" + string(ptr, len);
			return GetPointerUnsafe<yyjson_t>(root, str.c_str(), len);
		}
	}

	//! Get JSON value using JSON path query (unsafe)
	template <class yyjson_t>
	static inline yyjson_t *GetPointerUnsafe(yyjson_t *root, const char *ptr, const idx_t &len) {
		if (len == 0) {
			return nullptr;
		}
		switch (*ptr) {
		case '/':
			return TemplatedGetPointer<yyjson_t>(root, ptr, len);
		case '$':
			return TemplatedGetPointerDollar<yyjson_t>(root, ptr, len);
		default:
			throw InternalException("JSON path does not start with '/' or '$'");
		}
	}

public:
	//! Single-argument JSON read function, i.e. json_type('[1, 2, 3]')
	template <class T>
	static void UnaryExecute(DataChunk &args, ExpressionState &state, Vector &result,
	                         std::function<bool(yyjson_val *, T &, Vector &)> fun) {
		auto &inputs = args.data[0];
		UnaryExecutor::ExecuteWithNulls<string_t, T>(inputs, result, args.size(),
		                                             [&](string_t input, ValidityMask &mask, idx_t idx) {
			                                             auto doc = JSONCommon::ReadDocument(input);
			                                             T result_val {};
			                                             if (!fun(doc->root, result_val, result)) {
				                                             // Cannot find path
				                                             mask.SetInvalid(idx);
			                                             }
			                                             yyjson_doc_free(doc);
			                                             return result_val;
		                                             });
	}

	//! Two-argument JSON read function (with path query), i.e. json_type('[1, 2, 3]', '$[0]')
	template <class T>
	static void BinaryExecute(DataChunk &args, ExpressionState &state, Vector &result,
	                          std::function<bool(yyjson_val *, T &, Vector &)> fun) {
		auto &func_expr = (BoundFunctionExpression &)state.expr;
		const auto &info = (JSONReadFunctionData &)*func_expr.bind_info;

		auto &inputs = args.data[0];
		if (info.constant) {
			// Constant path
			const char *ptr = info.ptr;
			const idx_t &len = info.len;
			UnaryExecutor::ExecuteWithNulls<string_t, T>(
			    inputs, result, args.size(), [&](string_t input, ValidityMask &mask, idx_t idx) {
				    auto doc = ReadDocument(input);
				    T result_val {};
				    if (!fun(GetPointerUnsafe<yyjson_val>(doc->root, ptr, len), result_val, result)) {
					    // Cannot find path
					    mask.SetInvalid(idx);
				    }
				    yyjson_doc_free(doc);
				    return result_val;
			    });
		} else {
			// Columnref path
			auto &paths = args.data[1];
			BinaryExecutor::ExecuteWithNulls<string_t, string_t, T>(
			    inputs, paths, result, args.size(), [&](string_t input, string_t path, ValidityMask &mask, idx_t idx) {
				    auto doc = ReadDocument(input);
				    T result_val {};
				    if (!fun(GetPointer<yyjson_val>(doc->root, path), result_val, result)) {
					    // Cannot find path
					    mask.SetInvalid(idx);
				    }
				    yyjson_doc_free(doc);
				    return result_val;
			    });
		}
	}

	//! JSON read function with list of path queries, i.e. json_type('[1, 2, 3]', ['$[0]', '$[1]'])
	template <class T>
	static void ExecuteMany(DataChunk &args, ExpressionState &state, Vector &result,
	                        std::function<bool(yyjson_val *, T &, Vector &)> fun) {
		auto &func_expr = (BoundFunctionExpression &)state.expr;
		const auto &info = (JSONReadManyFunctionData &)*func_expr.bind_info;
		const auto &ptrs = info.ptrs;
		const auto &lens = info.lens;
		D_ASSERT(ptrs.size() == lens.size());
		const idx_t num_paths = ptrs.size();

		const auto count = args.size();
		VectorData input_data;
		auto &input_vector = args.data[0];
		input_vector.Orrify(count, input_data);
		auto inputs = (string_t *)input_data.data;

		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_entries = FlatVector::GetData<list_entry_t>(result);
		auto &result_validity = FlatVector::Validity(result);

		ListVector::Reserve(result, num_paths * count);
		ListVector::SetListSize(result, num_paths * count);

		auto &result_child = ListVector::GetEntry(result);
		auto result_child_data = FlatVector::GetData<T>(result_child);
		auto &result_child_validity = FlatVector::Validity(result_child);

		idx_t offset = 0;
		for (idx_t i = 0; i < count; i++) {
			result_entries[i].offset = offset;
			result_entries[i].length = num_paths;

			auto idx = input_data.sel->get_index(i);
			if (!input_data.validity.RowIsValid(idx)) {
				result_validity.SetInvalid(i);
				continue;
			}

			auto doc = ReadDocument(inputs[idx]);
			for (idx_t path_i = 0; path_i < num_paths; path_i++) {
				if (!fun(GetPointerUnsafe<yyjson_val>(doc->root, ptrs[path_i], lens[path_i]),
				         result_child_data[offset + path_i], result_child)) {
					result_child_validity.SetInvalid(offset);
				}
			}
			offset += num_paths;
			yyjson_doc_free(doc);
		}
		D_ASSERT(offset = num_paths * count);

		if (input_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	}

private:
	//! Get JSON pointer using /field/index/... notation
	template <class yyjson_t>
	static inline yyjson_t *TemplatedGetPointer(yyjson_t *root, const char *ptr, const idx_t &len) {
		throw InternalException("Unknown yyjson type");
	}

	//! Get JSON pointer using $.field[index]... notation
	template <class yyjson_t>
	static yyjson_t *TemplatedGetPointerDollar(yyjson_t *val, const char *ptr, const idx_t &len) {
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
				if (!IsObj<yyjson_t>(val)) {
					return nullptr;
				}
				bool escaped = false;
				if (*ptr == '"') {
					// Skip past opening '"'
					ptr++;
					escaped = true;
				}
				auto key_len = ReadString(ptr, end, escaped);
				val = ObjGetN<yyjson_t>(val, ptr, key_len);
				ptr += key_len;
				if (escaped) {
					// Skip past closing '"'
					ptr++;
				}
			} else if (c == '[') {
				// Array
				if (!IsArr<yyjson_t>(val)) {
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
					auto arr_size = ArrSize<yyjson_t>(val);
					idx = idx > arr_size ? arr_size : arr_size - idx;
				}
				val = ArrGet<yyjson_t>(val, idx);
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

	template <class yyjson_t>
	static inline bool IsObj(yyjson_t *val) {
		throw InternalException("Unknown yyjson type");
	}

	template <class yyjson_t>
	static inline yyjson_t *ObjGetN(yyjson_t *val, const char *ptr, idx_t key_len) {
		throw InternalException("Unknown yyjson type");
	}

	template <class yyjson_t>
	static inline bool IsArr(yyjson_t *val) {
		throw InternalException("Unknown yyjson type");
	}

	template <class yyjson_t>
	static inline size_t ArrSize(yyjson_t *val) {
		throw InternalException("Unknown yyjson type");
	}

	template <class yyjson_t>
	static inline yyjson_t *ArrGet(yyjson_t *val, idx_t index) {
		throw InternalException("Unknown yyjson type");
	}

public:
	static void ThrowValFormatError(string error_string, yyjson_val *val) {
		auto *mut_doc = yyjson_mut_doc_new(nullptr);
		auto *mut_val = yyjson_val_mut_copy(mut_doc, val);
		yyjson_mut_doc_set_root(mut_doc, mut_val);
		idx_t len;
		auto data = yyjson_mut_write(mut_doc, WRITE_FLAG, (size_t *)&len);
		error_string = StringUtil::Format(error_string, string(data, len));
		free(data);
		yyjson_mut_doc_free(mut_doc);
		throw InvalidInputException(error_string);
	}

	template <class T>
	static inline bool GetValueNumerical(yyjson_val *val, T &result, bool strict) {
		auto type = yyjson_get_type(val);
		switch (type) {
		case YYJSON_TYPE_NULL:
		case YYJSON_TYPE_NONE:
			return false;
		case YYJSON_TYPE_ARR:
		case YYJSON_TYPE_OBJ:
			break;
		default:
			if (GetValueNumerical<T>(val, result, type, strict)) {
				return true;
			}
			break;
		}
		if (strict) {
			ThrowValFormatError("Failed to cast value to numerical: %s", val);
		}
		return false;
	}

	static inline bool GetValueString(yyjson_val *val, string_t &result, Vector &vector) {
		switch (yyjson_get_type(val)) {
		case YYJSON_TYPE_NULL:
		case YYJSON_TYPE_NONE:
			return false;
		case YYJSON_TYPE_BOOL:
			result = StringCast::Operation<bool>(unsafe_yyjson_get_bool(val), vector);
			break;
		case YYJSON_TYPE_STR:
			result = StringVector::AddString(vector, unsafe_yyjson_get_str(val), unsafe_yyjson_get_len(val));
			break;
		case YYJSON_TYPE_NUM:
			switch (unsafe_yyjson_get_subtype(val)) {
			case YYJSON_SUBTYPE_UINT:
				result = StringCast::Operation<uint64_t>(unsafe_yyjson_get_uint(val), vector);
				break;
			case YYJSON_SUBTYPE_SINT:
				result = StringCast::Operation<int64_t>(unsafe_yyjson_get_sint(val), vector);
				break;
			case YYJSON_SUBTYPE_REAL:
				result = StringCast::Operation<double>(unsafe_yyjson_get_real(val), vector);
				break;
			default:
				throw InternalException("Unexpected yyjson subtype in GetValueString");
			}
			break;
		default:
			// Convert JSON object/arrays to strings because we can
			result = JSONCommon::WriteVal(val, vector);
		}
		// Casting to string should always work (if not NULL)
		return true;
	}

	template <class T>
	static inline bool GetValueDecimal(yyjson_val *val, T &result, uint8_t width, uint8_t scale, bool strict) {
		switch (yyjson_get_type(val)) {
		case YYJSON_TYPE_NULL:
		case YYJSON_TYPE_NONE:
			return false;
		case YYJSON_TYPE_ARR:
		case YYJSON_TYPE_OBJ:
			break;
		default:
			string error_message;
			if (GetValueDecimal<T>(val, result, &error_message, width, scale)) {
				return true;
			}
			break;
		}
		if (strict) {
			ThrowValFormatError("Failed to cast value to DECIMAL: %s", val);
		}
		return false;
	}

	static inline bool GetValueUUID(yyjson_val *val, hugeint_t &result, Vector &vector, bool strict) {
		switch (yyjson_get_type(val)) {
		case YYJSON_TYPE_NULL:
		case YYJSON_TYPE_NONE:
			return false;
		case YYJSON_TYPE_STR: {
			string error_message;
			if (TryCastToUUID::Operation(GetStringFromVal(val), result, vector, &error_message, strict)) {
				return true;
			}
			break;
		}
		case YYJSON_TYPE_NUM:
		case YYJSON_TYPE_BOOL:
		case YYJSON_TYPE_ARR:
		case YYJSON_TYPE_OBJ:
			break;
		default:
			throw InternalException("Unexpected yyjson type in GetValueUUID");
		}
		if (strict) {
			ThrowValFormatError("Failed to cast value to UUID: %s", val);
		}
		return false;
	}

	template <class T, class OP>
	static inline bool GetValueDateTime(yyjson_val *val, T &result, bool strict) {
		switch (yyjson_get_type(val)) {
		case YYJSON_TYPE_NULL:
		case YYJSON_TYPE_NONE:
			return false;
		case YYJSON_TYPE_STR: {
			string error_message;
			if (OP::template Operation<string_t, T>(GetStringFromVal(val), result, &error_message, strict)) {
				return true;
			}
			break;
		}
		case YYJSON_TYPE_NUM:
		case YYJSON_TYPE_BOOL:
		case YYJSON_TYPE_ARR:
		case YYJSON_TYPE_OBJ:
			break;
		default:
			throw InternalException("Unexpected yyjson type in GetValueDateTime");
		}
		if (strict) {
			ThrowValFormatError("Failed to cast value to DateTime: %s", val);
		}
		return false;
	}

	template <class OP>
	static inline bool GetValueTimestamp(yyjson_val *val, timestamp_t &result, bool strict) {
		switch (yyjson_get_type(val)) {
		case YYJSON_TYPE_NULL:
		case YYJSON_TYPE_NONE:
			return false;
		case YYJSON_TYPE_STR:
			if (OP::template Operation<string_t>(GetStringFromVal(val), result, strict)) {
				return true;
			}
			break;
		case YYJSON_TYPE_NUM:
		case YYJSON_TYPE_BOOL:
		case YYJSON_TYPE_ARR:
		case YYJSON_TYPE_OBJ:
			break;
		default:
			throw InternalException("Unexpected yyjson type in GetValueTimestamp");
		}
		if (strict) {
			ThrowValFormatError("Failed to cast value to Timestamp: %s", val);
		}
		return false;
	}

private:
	static inline string_t GetStringFromVal(yyjson_val *val) {
		return string_t(unsafe_yyjson_get_str(val), unsafe_yyjson_get_len(val));
	}

	template <class T>
	static inline bool GetValueNumerical(yyjson_val *val, T &result, yyjson_type type, bool strict) {
		bool success;
		switch (type) {
		case YYJSON_TYPE_BOOL:
			success = TryCast::Operation<bool, T>(unsafe_yyjson_get_bool(val), result, strict);
			break;
		case YYJSON_TYPE_STR:
			success = TryCast::Operation<string_t, T>(GetStringFromVal(val), result, strict);
			break;
		case YYJSON_TYPE_NUM:
			switch (unsafe_yyjson_get_subtype(val)) {
			case YYJSON_SUBTYPE_UINT:
				success = TryCast::Operation<uint64_t, T>(unsafe_yyjson_get_uint(val), result, strict);
				break;
			case YYJSON_SUBTYPE_SINT:
				success = TryCast::Operation<int64_t, T>(unsafe_yyjson_get_sint(val), result, strict);
				break;
			case YYJSON_SUBTYPE_REAL:
				success = TryCast::Operation<double, T>(unsafe_yyjson_get_real(val), result, strict);
				break;
			default:
				throw InternalException("Unexpected yyjson subtype in TemplatedGetValue");
			}
			break;
		default:
			throw InternalException("Unexpected yyjson type in TemplatedGetValue");
		}
		if (success) {
			return true;
		}
		if (strict) {
			ThrowValFormatError("Failed to cast value: %s", val);
		}
		return false;
	}

	template <class T>
	static inline bool GetValueDecimal(yyjson_val *val, T &result, string *error_message, uint8_t width,
	                                   uint8_t scale) {
		switch (yyjson_get_type(val)) {
		case YYJSON_TYPE_BOOL:
			return TryCastToDecimal::Operation<bool, T>(unsafe_yyjson_get_bool(val), result, error_message, width,
			                                            scale);
		case YYJSON_TYPE_STR:
			return TryCastToDecimal::Operation<string_t, T>(GetStringFromVal(val), result, error_message, width, scale);
		case YYJSON_TYPE_NUM:
			switch (unsafe_yyjson_get_subtype(val)) {
			case YYJSON_SUBTYPE_UINT:
				return TryCastToDecimal::Operation<uint64_t, T>(unsafe_yyjson_get_uint(val), result, error_message,
				                                                width, scale);
			case YYJSON_SUBTYPE_SINT:
				return TryCastToDecimal::Operation<int64_t, T>(unsafe_yyjson_get_sint(val), result, error_message,
				                                               width, scale);
			case YYJSON_SUBTYPE_REAL:
				return TryCastToDecimal::Operation<double, T>(unsafe_yyjson_get_real(val), result, error_message, width,
				                                              scale);
			default:
				throw InternalException("Unexpected yyjson subtype in GetValueDecimal");
			}
		default:
			throw InternalException("Unexpected yyjson type in GetValueDecimal");
		}
	}
};

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

} // namespace duckdb
