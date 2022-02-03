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
	const size_t len;
};

struct JSONReadManyFunctionData : public FunctionData {
public:
	explicit JSONReadManyFunctionData(vector<string> paths_p, vector<size_t> lens_p);
	unique_ptr<FunctionData> Copy() override;
	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments);

public:
	const vector<string> paths;
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
	//! Write JSON document
	static inline string_t WriteDocument(yyjson_val *val) {
		// Create mutable copy of the read val
		auto *mut_doc = yyjson_mut_doc_new(nullptr);
		auto *mut_val = yyjson_val_mut_copy(mut_doc, val);
		yyjson_mut_doc_set_root(mut_doc, mut_val);
		// Write mutable copy to string
		idx_t len;
		char *json = yyjson_mut_write(mut_doc, WRITE_FLAG, (size_t *)&len);
		yyjson_mut_doc_free(mut_doc);
		return string_t(json, len);
	}

public:
	//! Validate path with $ syntax
	static bool ValidPathDollar(const char *ptr, const idx_t &len);

	//! Get JSON value using JSON path query (safe, checks the path query)
	static inline yyjson_val *GetPointer(yyjson_val *root, const string_t &query) {
		auto ptr = query.GetDataUnsafe();
		auto len = query.GetSize();
		if (len == 0) {
			return GetPointerUnsafe(root, ptr, len);
		}
		switch (*ptr) {
		case '/': {
			// '/' notation must be '\0'-terminated
			auto str = string(ptr, len);
			return GetPointerUnsafe(root, str.c_str(), len);
		}
		case '$':
			if (!ValidPathDollar(ptr, len)) {
				throw Exception("JSON path error");
			}
			return GetPointerUnsafe(root, ptr, len);
		default:
			auto str = "/" + string(ptr, len);
			return GetPointerUnsafe(root, str.c_str(), len);
		}
	}

	//! Get JSON value using JSON path query (unsafe)
	static inline yyjson_val *GetPointerUnsafe(yyjson_val *root, const char *ptr, const idx_t &len) {
		if (len == 0) {
			return nullptr;
		}
		switch (*ptr) {
		case '/':
			return GetPointer(root, ptr, len);
		case '$':
			return GetPointerDollar(root, ptr, len);
		default:
			throw InternalException("JSON path does not start with '/' or '$'");
		}
	}

private:
	//! Get JSON pointer using /field/index/... notation
	static inline yyjson_val *GetPointer(yyjson_val *root, const char *ptr, const idx_t &len) {
		return len == 1 ? root : unsafe_yyjson_get_pointer(root, ptr, len);
	}
	//! Get JSON pointer using $.field[index]... notation
	static yyjson_val *GetPointerDollar(yyjson_val *val, const char *ptr, const idx_t &len);

public:
	//! Unary JSON function
	template <class T>
	static void UnaryJSONReadFunction(DataChunk &args, ExpressionState &state, Vector &result,
	                                  std::function<bool(yyjson_val *, T &)> fun) {
		auto &inputs = args.data[0];
		UnaryExecutor::ExecuteWithNulls<string_t, T>(inputs, result, args.size(),
		                                             [&](string_t input, ValidityMask &mask, idx_t idx) {
			                                             T result_val {};
			                                             auto doc = JSONCommon::ReadDocument(input);
			                                             auto root = doc->root;
			                                             if (!root || !fun(root, result_val)) {
				                                             mask.SetInvalid(idx);
			                                             }
			                                             yyjson_doc_free(doc);
			                                             return result_val;
		                                             });
	}

	//! Binary JSON function
	template <class T>
	static void BinaryJSONReadFunction(DataChunk &args, ExpressionState &state, Vector &result,
	                                   std::function<bool(yyjson_val *, T &)> fun) {
		auto &func_expr = (BoundFunctionExpression &)state.expr;
		const auto &info = (JSONReadFunctionData &)*func_expr.bind_info;

		auto &inputs = args.data[0];
		if (info.constant) {
			// Constant query
			const char *ptr = info.path.c_str();
			const idx_t &len = info.len;
			UnaryExecutor::ExecuteWithNulls<string_t, T>(
			    inputs, result, args.size(), [&](string_t input, ValidityMask &mask, idx_t idx) {
				    T result_val {};
				    auto doc = JSONCommon::ReadDocument(input);
				    auto root = doc->root;
				    if (!root || !fun(JSONCommon::GetPointerUnsafe(root, ptr, len), result_val)) {
					    mask.SetInvalid(idx);
				    }
				    yyjson_doc_free(doc);
				    return result_val;
			    });
		} else {
			// Columnref query
			auto &queries = args.data[1];
			BinaryExecutor::ExecuteWithNulls<string_t, string_t, T>(
			    inputs, queries, result, args.size(),
			    [&](string_t input, string_t query, ValidityMask &mask, idx_t idx) {
				    T result_val {};
				    auto doc = JSONCommon::ReadDocument(input);
				    auto root = doc->root;
				    if (!root || !fun(JSONCommon::GetPointer(root, query), result_val)) {
					    mask.SetInvalid(idx);
				    }
				    yyjson_doc_free(doc);
				    return result_val;
			    });
		}
	}

	//! Binary JSON function
	template <class T>
	static void JSONReadManyFunction(DataChunk &args, ExpressionState &state, Vector &result,
	                                 std::function<bool(yyjson_val *, T &)> fun) {
		auto &func_expr = (BoundFunctionExpression &)state.expr;
		const auto &info = (JSONReadFunctionData &)*func_expr.bind_info;

		auto &inputs = args.data[0];
		if (info.constant) {
			// Constant query
			const char *ptr = info.path.c_str();
			const idx_t &len = info.len;
			UnaryExecutor::ExecuteWithNulls<string_t, T>(
			    inputs, result, args.size(), [&](string_t input, ValidityMask &mask, idx_t idx) {
				    T result_val {};
				    auto doc = JSONCommon::ReadDocument(input);
				    auto root = doc->root;
				    if (!root || !fun(JSONCommon::GetPointerUnsafe(root, ptr, len), result_val)) {
					    mask.SetInvalid(idx);
				    }
				    yyjson_doc_free(doc);
				    return result_val;
			    });
		}
	}
};

} // namespace duckdb
