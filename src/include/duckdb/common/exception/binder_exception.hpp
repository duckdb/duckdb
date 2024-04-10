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

	template <typename... ARGS>
	explicit BinderException(const string &msg, ARGS... params) : BinderException(ConstructMessage(msg, params...)) {
	}
	template <typename... ARGS>
	explicit BinderException(const TableRef &ref, const string &msg, ARGS... params)
	    : BinderException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(ref)) {
	}
	template <typename... ARGS>
	explicit BinderException(const ParsedExpression &expr, const string &msg, ARGS... params)
	    : BinderException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(expr)) {
	}
	template <typename... ARGS>
	explicit BinderException(const Expression &expr, const string &msg, ARGS... params)
	    : BinderException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(expr)) {
	}
	template <typename... ARGS>
	explicit BinderException(QueryErrorContext error_context, const string &msg, ARGS... params)
	    : BinderException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(error_context)) {
	}
	template <typename... ARGS>
	explicit BinderException(optional_idx error_location, const string &msg, ARGS... params)
	    : BinderException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(error_location)) {
	}

	static BinderException ColumnNotFound(const string &name, const vector<string> &similar_bindings,
	                                      QueryErrorContext context = QueryErrorContext());
	static BinderException NoMatchingFunction(const string &name, const vector<LogicalType> &arguments,
	                                          const vector<string> &candidates);
};

} // namespace duckdb
