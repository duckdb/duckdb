//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/entry_lookup_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/parser/query_error_context.hpp"

namespace duckdb {

struct EntryLookupInfo {
public:
	EntryLookupInfo(CatalogType catalog_type, const string &name, QueryErrorContext error_context = QueryErrorContext());

public:
	CatalogType GetCatalogType() const;
	const string &GetEntryName() const;
	const QueryErrorContext &GetErrorContext() const;

private:
	CatalogType catalog_type;
	const string &name;
	QueryErrorContext error_context;
};

//! Return value of Catalog::LookupEntry
struct CatalogEntryLookup {
	optional_ptr<SchemaCatalogEntry> schema;
	optional_ptr<CatalogEntry> entry;
	ErrorData error;

	DUCKDB_API bool Found() const {
		return entry;
	}
};

} // namespace duckdb
