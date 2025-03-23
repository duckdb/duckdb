#include "duckdb/catalog/entry_lookup_info.hpp"

namespace duckdb {

EntryLookupInfo::EntryLookupInfo(CatalogType catalog_type_p, const string &name_p, QueryErrorContext error_context_p)
    : catalog_type(catalog_type_p), name(name_p), error_context(error_context_p) {
}

EntryLookupInfo::EntryLookupInfo(CatalogType catalog_type_p, const string &name_p,
                                 optional_ptr<BoundAtClause> at_clause_p, QueryErrorContext error_context_p)
    : catalog_type(catalog_type_p), name(name_p), at_clause(at_clause_p), error_context(error_context_p) {
}

EntryLookupInfo::EntryLookupInfo(const EntryLookupInfo &parent, const string &name_p)
    : catalog_type(parent.catalog_type), name(name_p), at_clause(parent.at_clause),
      error_context(parent.error_context) {
}

EntryLookupInfo::EntryLookupInfo(const EntryLookupInfo &parent, optional_ptr<BoundAtClause> at_clause)
    : EntryLookupInfo(parent.catalog_type, parent.name, parent.at_clause ? parent.at_clause : at_clause,
                      parent.error_context) {
}

EntryLookupInfo EntryLookupInfo::SchemaLookup(const EntryLookupInfo &parent, const string &schema_name) {
	return EntryLookupInfo(CatalogType::SCHEMA_ENTRY, schema_name, parent.at_clause, parent.error_context);
}

CatalogType EntryLookupInfo::GetCatalogType() const {
	return catalog_type;
}

const string &EntryLookupInfo::GetEntryName() const {
	return name;
}

const QueryErrorContext &EntryLookupInfo::GetErrorContext() const {
	return error_context;
}

const optional_ptr<BoundAtClause> EntryLookupInfo::GetAtClause() const {
	return at_clause;
}

} // namespace duckdb
