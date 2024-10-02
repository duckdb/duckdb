#include "duckdb/catalog/catalog_entry_retriever.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"

namespace duckdb {

LogicalType CatalogEntryRetriever::GetType(Catalog &catalog, const string &schema, const string &name,
                                           OnEntryNotFound on_entry_not_found) {
	QueryErrorContext error_context;
	auto result = GetEntry(CatalogType::TYPE_ENTRY, catalog, schema, name, on_entry_not_found, error_context);
	if (!result) {
		return LogicalType::INVALID;
	}
	auto &type_entry = result->Cast<TypeCatalogEntry>();
	return type_entry.user_type;
}

LogicalType CatalogEntryRetriever::GetType(const string &catalog, const string &schema, const string &name,
                                           OnEntryNotFound on_entry_not_found) {
	QueryErrorContext error_context;
	auto result = GetEntry(CatalogType::TYPE_ENTRY, catalog, schema, name, on_entry_not_found, error_context);
	if (!result) {
		return LogicalType::INVALID;
	}
	auto &type_entry = result->Cast<TypeCatalogEntry>();
	return type_entry.user_type;
}

optional_ptr<CatalogEntry> CatalogEntryRetriever::GetEntry(CatalogType type, const string &catalog,
                                                           const string &schema, const string &name,
                                                           OnEntryNotFound on_entry_not_found,
                                                           QueryErrorContext error_context) {
	return GetEntryInternal(
	    [&]() { return Catalog::GetEntry(context, type, catalog, schema, name, on_entry_not_found, error_context); });
}

optional_ptr<SchemaCatalogEntry> CatalogEntryRetriever::GetSchema(const string &catalog, const string &name,
                                                                  OnEntryNotFound on_entry_not_found,
                                                                  QueryErrorContext error_context) {
	auto result = Catalog::GetSchema(context, catalog, name, on_entry_not_found, error_context);
	if (!result) {
		return result;
	}
	if (callback) {
		// Call the callback if it's set
		callback(*result);
	}
	return result;
}

optional_ptr<CatalogEntry> CatalogEntryRetriever::GetEntry(CatalogType type, Catalog &catalog, const string &schema,
                                                           const string &name, OnEntryNotFound on_entry_not_found,
                                                           QueryErrorContext error_context) {
	return GetEntryInternal(
	    [&]() { return catalog.GetEntry(context, type, schema, name, on_entry_not_found, error_context); });
}

} // namespace duckdb
