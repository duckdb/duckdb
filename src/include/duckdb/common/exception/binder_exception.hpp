//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/exception/binder_exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/query_error_context.hpp"

namespace duckdb {

class BinderException : public Exception {
public:
	DUCKDB_API explicit BinderException(const string &msg, const unordered_map<string, string> &extra_info);
	DUCKDB_API explicit BinderException(const string &msg);

	template <typename... Args>
	explicit BinderException(const string &msg, Args... params) : BinderException(ConstructMessage(msg, params...)) {
	}
	template <typename... Args>
	explicit BinderException(const TableRef &ref, const string &msg, Args... params)
	    : BinderException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(ref)) {
	}
	template <typename... Args>
	explicit BinderException(const ParsedExpression &expr, const string &msg, Args... params)
	    : BinderException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(expr)) {
	}
	template <typename... Args>
	explicit BinderException(QueryErrorContext error_context, const string &msg, Args... params)
	    : BinderException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(error_context)) {
	}
	template <typename... Args>
	explicit BinderException(optional_idx error_location, const string &msg, Args... params)
	    : BinderException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(error_location)) {
	}

	static BinderException ColumnNotFound(const string &name, const vector<string> &similar_bindings,
	                                      QueryErrorContext context = QueryErrorContext());
	static BinderException NoMatchingFunction(const string &name, const vector<LogicalType> &arguments,
	                                          const vector<string> &candidates);
};

} // namespace duckdb
