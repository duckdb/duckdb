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
	static constexpr auto READ_FLAG = YYJSON_READ_ALLOW_INF_AND_NAN | YYJSON_READ_STOP_WHEN_DONE;
	static constexpr auto WRITE_FLAG = YYJSON_WRITE_ALLOW_INF_AND_NAN;

public:
	//! Read JSON document (returns nullptr if invalid JSON)
	static inline yyjson_doc *ReadDocumentUnsafe(const string_t &input) {
		return yyjson_read(input.GetDataUnsafe(), input.GetSize(), READ_FLAG);
	}
	//! Read JSON document (throws error if malformed JSON)
	static inline yyjson_doc *ReadDocument(const string_t &input) {
		auto result = ReadDocumentUnsafe(input);
		if (!result) {
			throw Exception("malformed JSON");
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
				throw Exception("JSON path error");
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
			return GetPointer<yyjson_t>(root, ptr, len);
		case '$':
			return GetPointerDollar<yyjson_t>(root, ptr, len);
		default:
			throw InternalException("JSON path does not start with '/' or '$'");
		}
	}

private:
	//! Get JSON pointer using /field/index/... notation
	template <class yyjson_t>
	static inline yyjson_t *GetPointer(yyjson_t *root, const char *ptr, const idx_t &len) {
		throw InternalException("Unknown yyjson type");
	}
	template <>
	inline yyjson_val *GetPointer(yyjson_val *root, const char *ptr, const idx_t &len) {
		return len == 1 ? root : unsafe_yyjson_get_pointer(root, ptr, len);
	}
	template <>
	inline yyjson_mut_val *GetPointer(yyjson_mut_val *root, const char *ptr, const idx_t &len) {
		return len == 1 ? root : unsafe_yyjson_mut_get_pointer(root, ptr, len);
	}

	//! Get JSON pointer using $.field[index]... notation
	template <class yyjson_t>
	static yyjson_t *GetPointerDollar(yyjson_t *val, const char *ptr, const idx_t &len) {
		throw InternalException("Unknown yyjson type");
	}
	static yyjson_val *GetPointerDollar(yyjson_val *val, const char *ptr, const idx_t &len);
	static yyjson_mut_val *GetPointerDollar(yyjson_mut_val *val, const char *ptr, const idx_t &len);

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

private:
	//! Helper functions for JSONReadManyFunction
	template <class T>
	static inline void PushBack(Vector &result, Vector &result_child, T val) {
		throw NotImplementedException("Cannot insert Value with this type into JSON result list");
	}

	inline void PushBack(Vector &result, Vector &result_child, uint64_t val) {
		ListVector::PushBack(result, Value::UBIGINT(move(val)));
	}

	inline void PushBack(Vector &result, Vector &result_child, string_t val) {
		Value to_insert(StringVector::AddString(result_child, val));
		ListVector::PushBack(result, to_insert);
	}
};

} // namespace duckdb
