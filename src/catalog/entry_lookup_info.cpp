#include "duckdb/catalog/entry_lookup_info.hpp"

namespace duckdb {

EntryLookupInfo::EntryLookupInfo(CatalogType catalog_type_p, const string &name_p, QueryErrorContext error_context_p)
    : catalog_type(catalog_type_p), name(name_p), error_context(error_context_p) {
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

} // namespace duckdb
