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
	static constexpr auto READ_FLAGS =
	    YYJSON_READ_ALLOW_COMMENTS | YYJSON_READ_ALLOW_INF_AND_NAN | YYJSON_READ_STOP_WHEN_DONE;

public:
	//! Convert JSON query string to JSON path query
	static inline bool ConvertToPath(const string_t &query, string &result, idx_t &len) {
		len = query.GetSize();
		if (len == 0) {
			return false;
		}
		const char *ptr = query.GetDataUnsafe();
		if (*ptr == '/') {
			// Already a path string
			// FIXME: avoid copying the string in this case - already OK
			result = query.GetString();
		} else if (*ptr == '$') {
			// Dollar/dot/brackets syntax
			// FIXME: this does not check for escaped strings, and does not support e.g. [#-1] : last array element
			result = StringUtil::Replace(string(ptr + 1, len - 1), ".", "/");
			result = StringUtil::Replace(result, "]", "");
			result = StringUtil::Replace(result, "[", "/");
			len = result.length();
		} else {
			// Plain tag/array index, prepend slash
			len++;
			result = "/" + query.GetString();
		}
		return true;
	}

	//! Get root of JSON document (nullptr if malformed JSON)
	static inline yyjson_val *GetRootUnsafe(const string_t &input) {
		return yyjson_doc_get_root(yyjson_read(input.GetDataUnsafe(), input.GetSize(), READ_FLAGS));
	}

	//! Get root of JSON document (throws error if malformed JSON)
	static inline yyjson_val *GetRoot(const string_t &input) {
		auto root = GetRootUnsafe(input);
		if (!root) {
			throw InvalidInputException("malformed JSON");
		}
		return root;
	}

	//! Get JSON value using JSON path query
	static inline yyjson_val *GetPointer(const string_t &input, const char *ptr, const idx_t &len) {
		return unsafe_yyjson_get_pointer(GetRoot(input), ptr, len);
	}
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
			auto value = ExpressionExecutor::EvaluateScalar(*arguments[1]);
			if (!value.TryCastAs(LogicalType::VARCHAR)) {
				throw InvalidInputException("JSON path");
			}
			auto query = value.GetValueUnsafe<string_t>();
			JSONCommon::ConvertToPath(query, path, len);
		}
		return make_unique<JSONFunctionData>(constant, path, len);
	}
};

} // namespace duckdb
