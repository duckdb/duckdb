#include "duckdb/catalog/catalog_entry_retriever.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

LogicalType CatalogEntryRetriever::GetType(Catalog &catalog, const string &schema, const string &name,
                                           OnEntryNotFound on_entry_not_found) {
	EntryLookupInfo lookup_info(CatalogType::TYPE_ENTRY, name);
	auto result = GetEntry(catalog, schema, lookup_info, on_entry_not_found);
	if (!result) {
		return LogicalType::INVALID;
	}
	auto &type_entry = result->Cast<TypeCatalogEntry>();
	return type_entry.user_type;
}

LogicalType CatalogEntryRetriever::GetType(const string &catalog, const string &schema, const string &name,
                                           OnEntryNotFound on_entry_not_found) {
	EntryLookupInfo lookup_info(CatalogType::TYPE_ENTRY, name);
	auto result = GetEntry(catalog, schema, lookup_info, on_entry_not_found);
	if (!result) {
		return LogicalType::INVALID;
	}
	auto &type_entry = result->Cast<TypeCatalogEntry>();
	return type_entry.user_type;
}

optional_ptr<CatalogEntry> CatalogEntryRetriever::GetEntry(const string &catalog, const string &schema,
                                                           const EntryLookupInfo &lookup_info,
                                                           OnEntryNotFound on_entry_not_found) {
	return ReturnAndCallback(Catalog::GetEntry(*this, catalog, schema, lookup_info, on_entry_not_found));
}

optional_ptr<SchemaCatalogEntry> CatalogEntryRetriever::GetSchema(const string &catalog,
                                                                  const EntryLookupInfo &schema_lookup_p,
                                                                  OnEntryNotFound on_entry_not_found) {
	EntryLookupInfo schema_lookup(schema_lookup_p, at_clause);
	auto result = Catalog::GetSchema(*this, catalog, schema_lookup, on_entry_not_found);
	if (!result) {
		return result;
	}
	if (callback) {
		// Call the callback if it's set
		callback(*result);
	}
	return result;
}

optional_ptr<CatalogEntry> CatalogEntryRetriever::GetEntry(Catalog &catalog, const string &schema,
                                                           const EntryLookupInfo &lookup_info,
                                                           OnEntryNotFound on_entry_not_found) {
	return ReturnAndCallback(catalog.GetEntry(*this, schema, lookup_info, on_entry_not_found));
}

optional_ptr<CatalogEntry> CatalogEntryRetriever::ReturnAndCallback(optional_ptr<CatalogEntry> result) {
	if (!result) {
		return result;
	}
	if (callback) {
		// Call the callback if it's set
		callback(*result);
	}
	return result;
}

void CatalogEntryRetriever::Inherit(const CatalogEntryRetriever &parent) {
	this->callback = parent.callback;
	this->search_path = parent.search_path;
	this->at_clause = parent.at_clause;
}

const CatalogSearchPath &CatalogEntryRetriever::GetSearchPath() const {
	if (search_path) {
		return *search_path;
	}
	return *ClientData::Get(context).catalog_search_path;
}

void CatalogEntryRetriever::SetSearchPath(vector<CatalogSearchEntry> entries) {
	vector<CatalogSearchEntry> new_path;
	for (auto &entry : entries) {
		if (IsInvalidCatalog(entry.catalog) || entry.catalog == SYSTEM_CATALOG || entry.catalog == TEMP_CATALOG) {
			continue;
		}
		new_path.push_back(std::move(entry));
	}
	if (new_path.empty()) {
		return;
	}

	// push the set paths from the ClientContext behind the provided paths
	auto &client_search_path = *ClientData::Get(context).catalog_search_path;
	auto &set_paths = client_search_path.GetSetPaths();
	for (auto path : set_paths) {
		if (IsInvalidCatalog(path.catalog)) {
			path.catalog = DatabaseManager::GetDefaultDatabase(context);
		}
		new_path.push_back(std::move(path));
	}

	this->search_path = make_shared_ptr<CatalogSearchPath>(context, std::move(new_path));
}

optional_ptr<BoundAtClause> CatalogEntryRetriever::GetAtClause() const {
	return at_clause;
}

void CatalogEntryRetriever::SetAtClause(optional_ptr<BoundAtClause> at_clause_p) {
	at_clause = at_clause_p;
}

void CatalogEntryRetriever::SetCallback(catalog_entry_callback_t callback) {
	this->callback = std::move(callback);
}

catalog_entry_callback_t CatalogEntryRetriever::GetCallback() {
	return callback;
}

} // namespace duckdb
