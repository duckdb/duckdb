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

class CatalogException : public StandardException {
public:
	DUCKDB_API explicit CatalogException(const string &msg);

	template <typename... Args>
	explicit CatalogException(const string &msg, Args... params) : CatalogException(ConstructMessage(msg, params...)) {
	}

	static CatalogException MissingEntry(CatalogType type, const string &name, const string &suggestion, QueryErrorContext context = QueryErrorContext());
	static CatalogException MissingEntry(const string &type, const string &name, const vector<string> &suggestions, QueryErrorContext context = QueryErrorContext());
	static CatalogException EntryAlreadyExists(CatalogType type, const string &name, QueryErrorContext context = QueryErrorContext());
};

} // namespace duckdb
