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
	                                    const string &name,
	                                    OnEntryNotFound on_entry_not_found = OnEntryNotFound::THROW_EXCEPTION,
	                                    QueryErrorContext error_context = QueryErrorContext()) {
		return GetEntryInternal([&]() {
			return Catalog::GetEntry(context, type, catalog, schema, name, on_entry_not_found, error_context);
		});
	}

	optional_ptr<CatalogEntry> GetEntry(CatalogType type, Catalog &catalog, const string &schema, const string &name,
	                                    OnEntryNotFound on_entry_not_found = OnEntryNotFound::THROW_EXCEPTION,
	                                    QueryErrorContext error_context = QueryErrorContext());

	LogicalType GetType(const string &catalog, const string &schema, const string &name,
	                    OnEntryNotFound on_entry_not_found = OnEntryNotFound::RETURN_NULL);
	LogicalType GetType(Catalog &catalog, const string &schema, const string &name,
	                    OnEntryNotFound on_entry_not_found = OnEntryNotFound::RETURN_NULL);

	void SetCallback(catalog_entry_callback_t callback) {
		this->callback = callback;
	}
	catalog_entry_callback_t GetCallback() {
		return callback;
	}

private:
	using catalog_entry_retrieve_func_t = std::function<optional_ptr<CatalogEntry>()>;
	optional_ptr<CatalogEntry> GetEntryInternal(catalog_entry_retrieve_func_t retriever) {
		auto result = retriever();
		if (!result) {
			return result;
		}
		if (callback) {
			// Call the callback if it's set
			callback(*result);
		}
		return result;
	}

private:
	//! (optional) callback, called on every succesful entry retrieval
	catalog_entry_callback_t callback = nullptr;
	ClientContext &context;
};

} // namespace duckdb
