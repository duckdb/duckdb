#pragma once

#include <functional>
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/parser/query_error_context.hpp"

namespace duckdb {

class ClientContext;
class Catalog;
class CatalogEntry;

using catalog_entry_callback_t = std::function<void(CatalogEntry &)>;

// Wraps the Catalog::GetEntry method
class CatalogEntryRetriever {
public:
	CatalogEntryRetriever(ClientContext &context) : context(context) {
	}
	CatalogEntryRetriever(const CatalogEntryRetriever &other) : callback(other.callback), context(other.context) {
	}

public:
	optional_ptr<CatalogEntry> GetEntry(CatalogType type, const string &catalog, const string &schema,
	                                    const string &name, OnEntryNotFound on_entry_not_found,
	                                    QueryErrorContext &error_context) {
		auto result = Catalog::GetEntry(context, type, catalog, schema, name, on_entry_not_found, error_context);
		if (!result) {
			return result;
		}
		if (callback) {
			// Call the callback if it's set
			callback(*result);
		}
		return result;
	}

	void SetCallback(catalog_entry_callback_t callback) {
		this->callback = callback;
	}
	catalog_entry_callback_t GetCallback() {
		return callback;
	}

private:
	//! (optional) callback, called on every succesful entry retrieval
	catalog_entry_callback_t callback = nullptr;
	ClientContext &context;
};

} // namespace duckdb
