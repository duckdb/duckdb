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
	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
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
	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments);

public:
	const vector<string> paths;
	vector<const char *> ptrs;
	const vector<size_t> lens;
};

template <class YYJSON_DOC_T>
static inline void CleanupDoc(YYJSON_DOC_T *doc) {
	throw InternalException("Unknown yyjson document type");
}

template <>
inline void CleanupDoc(yyjson_doc *doc) {
	yyjson_doc_free(doc);
}

template <>
inline void CleanupDoc(yyjson_mut_doc *doc) {
	yyjson_mut_doc_free(doc);
}

template <class YYJSON_DOC_T>
class DocPointer {
private:
	YYJSON_DOC_T *doc;

public:
	explicit DocPointer(YYJSON_DOC_T *doc) : doc(doc) {
	}

	DocPointer(const DocPointer &obj) = delete;
	DocPointer &operator=(const DocPointer &obj) = delete;

	DocPointer(DocPointer &&other) noexcept {
		this->doc = other.doc;
		other.doc = nullptr;
	}

	void operator=(DocPointer &&other) noexcept {
		CleanupDoc<YYJSON_DOC_T>(doc);
		this->ptr = other.ptr;
		other.ptr = nullptr;
	}

	inline YYJSON_DOC_T *operator*() const {
		return doc;
	}

	inline YYJSON_DOC_T *operator->() const {
		return doc;
	}

	inline bool IsNull() const {
		return doc == nullptr;
	}

	~DocPointer() {
		CleanupDoc<YYJSON_DOC_T>(doc);
	}
};

struct JSONCommon {
private:
	//! Read/Write flag that make sense for us
	static constexpr auto READ_FLAG = YYJSON_READ_ALLOW_INF_AND_NAN | YYJSON_READ_ALLOW_TRAILING_COMMAS;
	static constexpr auto WRITE_FLAG = YYJSON_WRITE_ALLOW_INF_AND_NAN;

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

public:
	static inline DocPointer<yyjson_mut_doc> CreateDocument() {
		return DocPointer<yyjson_mut_doc>(yyjson_mut_doc_new(nullptr));
	}

	//! Read JSON document (returns nullptr if invalid JSON)
	static inline DocPointer<yyjson_doc> ReadDocumentUnsafe(const string_t &input) {
		return DocPointer<yyjson_doc>(yyjson_read(input.GetDataUnsafe(), input.GetSize(), READ_FLAG));
	}
	//! Read JSON document (throws error if malformed JSON)
	static inline DocPointer<yyjson_doc> ReadDocument(const string_t &input) {
		auto result = ReadDocumentUnsafe(input);
		if (result.IsNull()) {
			throw InvalidInputException("malformed JSON");
		}
		return result;
	}
	//! Some wrappers around writes so we don't have to free the malloc'ed char[]
	static inline unique_ptr<char, void (*)(void *)> WriteVal(yyjson_val *val, idx_t &len) {
		return unique_ptr<char, decltype(free) *>(
		    reinterpret_cast<char *>(yyjson_val_write(val, WRITE_FLAG, (size_t *)&len)), free);
	}
	static unique_ptr<char, void (*)(void *)> WriteMutDoc(yyjson_mut_doc *doc, idx_t &len) {
		return unique_ptr<char, decltype(free) *>(
		    reinterpret_cast<char *>(yyjson_mut_write(doc, WRITE_FLAG, (size_t *)&len)), free);
	}
	//! Vector writes
	static inline string_t WriteVal(yyjson_val *val, Vector &vector) {
		idx_t len;
		auto data = WriteVal(val, len);
		return StringVector::AddString(vector, data.get(), len);
	}
	static inline string_t WriteDoc(yyjson_mut_doc *doc, Vector &vector) {
		idx_t len;
		auto data = WriteMutDoc(doc, len);
		return StringVector::AddString(vector, data.get(), len);
	}
	//! Throw an error with the printed yyjson_val
	static void ThrowValFormatError(string error_string, yyjson_val *val) {
		idx_t len;
		auto data = WriteVal(val, len);
		error_string = StringUtil::Format(error_string, string(data.get(), len));
		throw InvalidInputException(error_string);
	}

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
			return GetPointerUnsafe<YYJSON_VAL_T>(root, str.c_str(), len);
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

public:
	//! Single-argument JSON read function, i.e. json_type('[1, 2, 3]')
	template <class T>
	static void UnaryExecute(DataChunk &args, ExpressionState &state, Vector &result,
	                         std::function<T(yyjson_val *, Vector &)> fun) {
		auto &inputs = args.data[0];
		UnaryExecutor::Execute<string_t, T>(inputs, result, args.size(), [&](string_t input) {
			auto doc = JSONCommon::ReadDocument(input);
			return fun(doc->root, result);
		});
	}

	//! Two-argument JSON read function (with path query), i.e. json_type('[1, 2, 3]', '$[0]')
	template <class T>
	static void BinaryExecute(DataChunk &args, ExpressionState &state, Vector &result,
	                          std::function<T(yyjson_val *, Vector &)> fun) {
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
				    yyjson_val *val;
				    if (!(val = GetPointerUnsafe<yyjson_val>(doc->root, ptr, len))) {
					    mask.SetInvalid(idx);
					    return T {};
				    } else {
					    return fun(val, result);
				    }
			    });
		} else {
			// Columnref path
			auto &paths = args.data[1];
			BinaryExecutor::ExecuteWithNulls<string_t, string_t, T>(
			    inputs, paths, result, args.size(), [&](string_t input, string_t path, ValidityMask &mask, idx_t idx) {
				    auto doc = ReadDocument(input);
				    yyjson_val *val;
				    if (!(val = GetPointer<yyjson_val>(doc->root, path))) {
					    mask.SetInvalid(idx);
					    return T {};
				    } else {
					    return fun(val, result);
				    }
			    });
		}
	}

	//! JSON read function with list of path queries, i.e. json_type('[1, 2, 3]', ['$[0]', '$[1]'])
	template <class T>
	static void ExecuteMany(DataChunk &args, ExpressionState &state, Vector &result,
	                        std::function<T(yyjson_val *, Vector &)> fun) {
		auto &func_expr = (BoundFunctionExpression &)state.expr;
		const auto &info = (JSONReadManyFunctionData &)*func_expr.bind_info;
		D_ASSERT(info.ptrs.size() == info.lens.size());

		const auto count = args.size();
		const idx_t num_paths = info.ptrs.size();
		const idx_t list_size = count * num_paths;

		UnifiedVectorFormat input_data;
		auto &input_vector = args.data[0];
		input_vector.ToUnifiedFormat(count, input_data);
		auto inputs = (string_t *)input_data.data;

		ListVector::Reserve(result, list_size);
		auto list_entries = FlatVector::GetData<list_entry_t>(result);
		auto &list_validity = FlatVector::Validity(result);

		auto &child = ListVector::GetEntry(result);
		auto child_data = FlatVector::GetData<T>(child);
		auto &child_validity = FlatVector::Validity(child);

		idx_t offset = 0;
		yyjson_val *val;
		for (idx_t i = 0; i < count; i++) {
			auto idx = input_data.sel->get_index(i);
			if (!input_data.validity.RowIsValid(idx)) {
				list_validity.SetInvalid(i);
				continue;
			}

			auto doc = ReadDocument(inputs[idx]);
			for (idx_t path_i = 0; path_i < num_paths; path_i++) {
				auto child_idx = offset + path_i;
				if (!(val = GetPointerUnsafe<yyjson_val>(doc->root, info.ptrs[path_i], info.lens[path_i]))) {
					child_validity.SetInvalid(child_idx);
				} else {
					child_data[child_idx] = fun(val, child);
				}
			}

			list_entries[i].offset = offset;
			list_entries[i].length = num_paths;
			offset += num_paths;
		}
		ListVector::SetListSize(result, offset);
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
