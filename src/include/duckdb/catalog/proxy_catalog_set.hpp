#pragma once

#include "duckdb/catalog/catalog_set.hpp"

namespace duckdb {

//! This class mocks the CatalogSet interface, but does not actually store CatalogEntries
class ProxyCatalogSet {
public:
	ProxyCatalogSet(CatalogSet &set, const string &entry_prefix) : set(set), prefix(entry_prefix) {
	}

public:
	bool CreateEntry(CatalogTransaction transaction, const string &name, unique_ptr<CatalogEntry> value,
	                 const DependencyList &dependencies);
	CatalogSet::EntryLookup GetEntryDetailed(CatalogTransaction transaction, const string &name);
	optional_ptr<CatalogEntry> GetEntry(CatalogTransaction transaction, const string &name);
	void Scan(CatalogTransaction transaction, const std::function<void(CatalogEntry &)> &callback);
	bool DropEntry(CatalogTransaction transaction, const string &name, bool cascade, bool allow_drop_internal = false);

public:
	CatalogSet &set;
	string prefix;
};

} // namespace duckdb
