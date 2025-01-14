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
	return ReturnAndCallback(Catalog::GetEntry(*this, type, catalog, schema, name, on_entry_not_found, error_context));
}

optional_ptr<SchemaCatalogEntry> CatalogEntryRetriever::GetSchema(const string &catalog, const string &name,
                                                                  OnEntryNotFound on_entry_not_found,
                                                                  QueryErrorContext error_context) {
	auto result = Catalog::GetSchema(*this, catalog, name, on_entry_not_found, error_context);
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
	return ReturnAndCallback(catalog.GetEntry(*this, type, schema, name, on_entry_not_found, error_context));
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
}

CatalogSearchPath &CatalogEntryRetriever::GetSearchPath() {
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

void CatalogEntryRetriever::SetCallback(catalog_entry_callback_t callback) {
	this->callback = std::move(callback);
}

catalog_entry_callback_t CatalogEntryRetriever::GetCallback() {
	return callback;
}

} // namespace duckdb
