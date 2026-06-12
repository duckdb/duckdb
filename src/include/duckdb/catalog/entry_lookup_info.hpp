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
class BoundAtClause;

struct EntryLookupInfo {
public:
	EntryLookupInfo(CatalogType catalog_type, Identifier name, QueryErrorContext error_context = QueryErrorContext());
	EntryLookupInfo(CatalogType catalog_type, Identifier name, optional_ptr<BoundAtClause> at_clause,
	                QueryErrorContext error_context);
	EntryLookupInfo(const EntryLookupInfo &parent, Identifier name);
	EntryLookupInfo(const EntryLookupInfo &parent, optional_ptr<BoundAtClause> at_clause);

public:
	CatalogType GetCatalogType() const;
	//! The identifier being looked up (the catalog stores/compares names case-insensitively).
	const Identifier &GetEntryIdentifier() const;
	//! The raw name of the identifier being looked up.
	const string &GetEntryName() const;
	const QueryErrorContext &GetErrorContext() const;
	const optional_ptr<BoundAtClause> GetAtClause() const;

	static EntryLookupInfo SchemaLookup(const EntryLookupInfo &parent, Identifier schema_name);

private:
	CatalogType catalog_type;
	Identifier name;
	optional_ptr<BoundAtClause> at_clause;
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
