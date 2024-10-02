//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/exception/catalog_exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

class CatalogException : public Exception {
public:
	DUCKDB_API explicit CatalogException(const string &msg);
	DUCKDB_API explicit CatalogException(const string &msg, const unordered_map<string, string> &extra_info);

	template <typename... ARGS>
	explicit CatalogException(const string &msg, ARGS... params) : CatalogException(ConstructMessage(msg, params...)) {
	}
	template <typename... ARGS>
	explicit CatalogException(QueryErrorContext error_context, const string &msg, ARGS... params)
	    : CatalogException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(error_context)) {
	}

	static CatalogException MissingEntry(CatalogType type, const string &name, const string &suggestion,
	                                     QueryErrorContext context = QueryErrorContext());
	static CatalogException MissingEntry(const string &type, const string &name, const vector<string> &suggestions,
	                                     QueryErrorContext context = QueryErrorContext());
	static CatalogException EntryAlreadyExists(CatalogType type, const string &name,
	                                           QueryErrorContext context = QueryErrorContext());
};

} // namespace duckdb
