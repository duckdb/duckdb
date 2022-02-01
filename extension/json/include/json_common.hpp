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

struct JSONCommon {
private:
	static constexpr auto READ_FLAGS = YYJSON_READ_ALLOW_INF_AND_NAN | YYJSON_READ_STOP_WHEN_DONE;

public:
	//! Get root of JSON document (returns nullptr if malformed JSON)
	static inline yyjson_val *GetRootUnsafe(const string_t &input) {
		return yyjson_doc_get_root(yyjson_read(input.GetDataUnsafe(), input.GetSize(), READ_FLAGS));
	}

	//! Get root of JSON document (throws error if malformed JSON)
	static inline yyjson_val *GetRoot(const string_t &input) {
		auto root = GetRootUnsafe(input);
		if (!root) {
			throw Exception("malformed JSON");
		}
		return root;
	}

public:
	//! Validate path with $ syntax
	static bool ValidPathDollar(const char *ptr, const idx_t &len);

	//! Get JSON value using JSON path query (safe, checks the path query)
	static inline yyjson_val *GetPointer(const string_t &input, const string_t &query) {
		auto ptr = query.GetDataUnsafe();
		auto len = query.GetSize();
		if (len == 0) {
			return GetPointerUnsafe(input, ptr, len);
		}
		switch (*ptr) {
		case '/': {
			// '/' notation must be '\0'-terminated
			auto str = string(ptr, len);
			return GetPointerUnsafe(input, str.c_str(), len);
		}
		case '$':
			if (!ValidPathDollar(ptr, len)) {
				throw Exception("JSON path error");
			}
			return GetPointerUnsafe(input, ptr, len);
		default:
			auto str = "/" + string(ptr, len);
			return GetPointerUnsafe(input, str.c_str(), len);
		}
	}

	//! Get JSON value using JSON path query (unsafe)
	static inline yyjson_val *GetPointerUnsafe(const string_t &input, const char *ptr, const idx_t &len) {
		if (len == 0) {
			return nullptr;
		}
		auto root = GetRoot(input);
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
};

struct JSONFunctionData : public FunctionData {
public:
	explicit JSONFunctionData(bool constant, string path_p, idx_t len)
	    : constant(constant), path(move(path_p)), len(len) {
	}

public:
	const bool constant;
	const string path;
	const size_t len;

public:
	unique_ptr<FunctionData> Copy() override {
		return make_unique<JSONFunctionData>(constant, path, len);
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		D_ASSERT(bound_function.arguments.size() == 2);
		bool constant = false;
		string path = "";
		idx_t len = 0;
		if (arguments[1]->return_type.id() != LogicalTypeId::SQLNULL && arguments[1]->IsFoldable()) {
			constant = true;
			// Try to cast to string, so that we can allow integers as arguments (array index)
			auto value = ExpressionExecutor::EvaluateScalar(*arguments[1]);
			if (!value.TryCastAs(LogicalType::VARCHAR)) {
				throw Exception("Cannot JSON path argument to VARCHAR");
			}
			// Get the string
			auto query = value.GetValueUnsafe<string_t>();
			len = query.GetSize();
			auto ptr = query.GetDataUnsafe();
			// Empty strings and invalid $ paths yield an error
			if (len == 0 || (*ptr == '$' && !JSONCommon::ValidPathDollar(ptr, len))) {
				throw Exception("JSON path error");
			}
			// Copy over string to the bind data
			if (*ptr == '/' || *ptr == '$') {
				path = string(ptr, len);
			} else {
				path = "/" + string(ptr, len);
				len++;
			}
		}
		return make_unique<JSONFunctionData>(constant, path, len);
	}
};

} // namespace duckdb
