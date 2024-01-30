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

	static BinderException ColumnNotFound(const string &name, const vector<string> &similar_bindings,
										 QueryErrorContext context = QueryErrorContext());
};

} // namespace duckdb
