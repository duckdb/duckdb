//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

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
	static inline string_t WriteVal(yyjson_mut_doc *doc) {
		idx_t len;
		char *json = yyjson_mut_write(doc, WRITE_FLAG, (size_t *)&len);
		return string_t(json, len);
	}
	static inline string_t WriteVal(yyjson_val *val) {
		// Create mutable copy of the read val
		auto *mut_doc = yyjson_mut_doc_new(nullptr);
		auto *mut_val = yyjson_val_mut_copy(mut_doc, val);
		yyjson_mut_doc_set_root(mut_doc, mut_val);
		// Write mutable copy to string
		auto result = WriteVal(mut_doc);
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
				return nullptr;
			}
		case YYJSON_TYPE_STR:
			return JSONCommon::TYPE_STRING_VARCHAR;
		case YYJSON_TYPE_ARR:
			return JSONCommon::TYPE_STRING_ARRAY;
		case YYJSON_TYPE_OBJ:
			return JSONCommon::TYPE_STRING_OBJECT;
		default:
			return nullptr;
		}
	}

	template <class T>
	static inline bool TemplatedGetValue(yyjson_val *val, T &result) {
		throw NotImplementedException("Cannot extract JSON of this type");
	}

public:
	//! Validate path with $ syntax
	static bool ValidPathDollar(const char *ptr, const idx_t &len);

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
		case '$':
			if (!ValidPathDollar(ptr, len)) {
				throw InvalidInputException("JSON path error");
			}
			return GetPointerUnsafe<yyjson_t>(root, ptr, len);
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
	static void UnaryJSONReadFunction(DataChunk &args, ExpressionState &state, Vector &result,
	                                  std::function<bool(yyjson_val *, T &)> fun) {
		auto &inputs = args.data[0];
		UnaryExecutor::ExecuteWithNulls<string_t, T>(inputs, result, args.size(),
		                                             [&](string_t input, ValidityMask &mask, idx_t idx) {
			                                             auto doc = JSONCommon::ReadDocument(input);
			                                             T result_val {};
			                                             if (!fun(doc->root, result_val)) {
				                                             // Cannot find path
				                                             mask.SetInvalid(idx);
			                                             }
			                                             yyjson_doc_free(doc);
			                                             return result_val;
		                                             });
	}

	//! Two-argument JSON read function (with path query), i.e. json_type('[1, 2, 3]', '$[0]')
	template <class T>
	static void BinaryJSONReadFunction(DataChunk &args, ExpressionState &state, Vector &result,
	                                   std::function<bool(yyjson_val *, T &)> fun) {
		auto &func_expr = (BoundFunctionExpression &)state.expr;
		const auto &info = (JSONReadFunctionData &)*func_expr.bind_info;

		auto &inputs = args.data[0];
		if (info.constant) {
			// Constant path
			const char *ptr = info.ptr;
			const idx_t &len = info.len;
			UnaryExecutor::ExecuteWithNulls<string_t, T>(
			    inputs, result, args.size(), [&](string_t input, ValidityMask &mask, idx_t idx) {
				    auto doc = JSONCommon::ReadDocument(input);
				    T result_val {};
				    if (!fun(JSONCommon::GetPointerUnsafe<yyjson_val>(doc->root, ptr, len), result_val)) {
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
				    auto doc = JSONCommon::ReadDocument(input);
				    T result_val {};
				    if (!fun(JSONCommon::GetPointer<yyjson_val>(doc->root, path), result_val)) {
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
	static void JSONReadManyFunction(DataChunk &args, ExpressionState &state, Vector &result,
	                                 std::function<bool(yyjson_val *, T &)> fun) {
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
		auto &result_child = ListVector::GetEntry(result);

		idx_t offset = 0;
		for (idx_t i = 0; i < count; i++) {
			auto idx = input_data.sel->get_index(i);
			if (!input_data.validity.RowIsValid(idx)) {
				result_validity.SetInvalid(i);
				continue;
			}
			auto doc = JSONCommon::ReadDocument(inputs[idx]);
			for (idx_t path_i = 0; path_i < num_paths; path_i++) {
				T result_val {};
				if (!fun(JSONCommon::GetPointerUnsafe<yyjson_val>(doc->root, ptrs[path_i], lens[path_i]), result_val)) {
					// Cannot find path
					ListVector::PushBack(result, Value());
				} else {
					PushBack<T>(result, result_child, move(result_val));
				}
			}
			yyjson_doc_free(doc);
			result_entries[i].offset = offset;
			result_entries[i].length = num_paths;
			offset += num_paths;
		}
		D_ASSERT(ListVector::GetListSize(result) == offset);

		if (input_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	}

public:
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

	// FIXME: we don't need to pushback because we know the list size
	//  We should do ListVector::Reserve instead
	template <class T>
	static inline void PushBack(Vector &result, Vector &result_child, T val) {
		throw NotImplementedException("Cannot insert Value with this type into JSON result list");
	}
};

template <>
inline bool JSONCommon::TemplatedGetValue(yyjson_val *val, bool &result) {
	auto valid = yyjson_is_bool(val);
	if (valid) {
		result = unsafe_yyjson_get_bool(val);
	}
	return valid;
}

template <>
inline bool JSONCommon::TemplatedGetValue(yyjson_val *val, int32_t &result) {
	auto valid = yyjson_is_int(val);
	if (valid) {
		result = unsafe_yyjson_get_int(val);
	}
	return valid;
}

template <>
inline bool JSONCommon::TemplatedGetValue(yyjson_val *val, int64_t &result) {
	// Needs to check whether it is int first, otherwise we get NULL for small values
	auto valid = yyjson_is_int(val) || yyjson_is_sint(val);
	if (valid) {
		result = unsafe_yyjson_get_sint(val);
	}
	return valid;
}

template <>
inline bool JSONCommon::TemplatedGetValue(yyjson_val *val, uint64_t &result) {
	auto valid = yyjson_is_uint(val);
	if (valid) {
		result = unsafe_yyjson_get_uint(val);
	}
	return valid;
}

template <>
inline bool JSONCommon::TemplatedGetValue(yyjson_val *val, double &result) {
	auto valid = yyjson_is_real(val);
	if (valid) {
		result = unsafe_yyjson_get_real(val);
	}
	return valid;
}

template <>
inline bool JSONCommon::TemplatedGetValue(yyjson_val *val, string_t &result) {
	auto valid = yyjson_is_str(val);
	if (valid) {
		result = string_t(unsafe_yyjson_get_str(val), unsafe_yyjson_get_len(val));
	}
	return valid;
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
inline void JSONCommon::PushBack(Vector &result, Vector &result_child, uint64_t val) {
	ListVector::PushBack(result, Value::UBIGINT(move(val)));
}
template <>
inline void JSONCommon::PushBack(Vector &result, Vector &result_child, string_t val) {
	Value to_insert(StringVector::AddString(result_child, val));
	ListVector::PushBack(result, to_insert);
}

} // namespace duckdb
