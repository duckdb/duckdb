#pragma once

#include "duckdb/catalog/catalog_set.hpp"

namespace duckdb {

//! This class mocks the CatalogSet interface, but does not actually store CatalogEntries
class ProxyCatalogSet {
public:
	ProxyCatalogSet(CatalogSet &set, const string &mangled_name, CatalogType type, const string &schema,
	                const string &name)
	    : set(set), mangled_name(mangled_name), type(type), schema(schema), name(name) {
	}

public:
	bool CreateEntry(CatalogTransaction transaction, const string &name, unique_ptr<CatalogEntry> value,
	                 const DependencyList &dependencies);
	CatalogSet::EntryLookup GetEntryDetailed(CatalogTransaction transaction, const string &name);
	optional_ptr<CatalogEntry> GetEntry(CatalogTransaction transaction, const string &name);
	void Scan(CatalogTransaction transaction, const std::function<void(CatalogEntry &)> &callback);
	bool DropEntry(CatalogTransaction transaction, const string &name, bool cascade, bool allow_drop_internal = false);

private:
	string ApplyPrefix(const string &name) const;

public:
	CatalogSet &set;
	string mangled_name;
	CatalogType type;
	string schema;
	string name;
};

} // namespace duckdb
