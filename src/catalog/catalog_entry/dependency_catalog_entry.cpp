#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency_set_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/dependency_manager.hpp"

namespace duckdb {

DependencyCatalogEntry::DependencyCatalogEntry(DependencyLinkSide side, Catalog &catalog,
                                               DependencySetCatalogEntry &set, CatalogType entry_type,
                                               const string &entry_schema, const string &entry_name,
                                               DependencyType dependency_type)
    : InCatalogEntry(CatalogType::DEPENDENCY_ENTRY, catalog,
                     DependencyManager::MangleName(entry_type, entry_schema, entry_name)),
      entry_name(entry_name), schema(entry_schema), entry_type(entry_type), dependency_type(dependency_type),
      side(side), set(set) {
	D_ASSERT(entry_type != CatalogType::DEPENDENCY_ENTRY);
	D_ASSERT(entry_type != CatalogType::DEPENDENCY_SET);
}

const string &DependencyCatalogEntry::MangledName() const {
	return name;
}

CatalogType DependencyCatalogEntry::EntryType() const {
	return entry_type;
}

const string &DependencyCatalogEntry::EntrySchema() const {
	return schema;
}

const string &DependencyCatalogEntry::EntryName() const {
	return entry_name;
}

DependencyType DependencyCatalogEntry::Type() const {
	return dependency_type;
}

DependencyCatalogEntry::~DependencyCatalogEntry() {
}

void DependencyCatalogEntry::CompleteLink(CatalogTransaction transaction, DependencyType type) {
	auto &manager = set.Manager();
	switch (side) {
	case DependencyLinkSide::DEPENDENCY: {
		auto &other_set = manager.GetOrCreateDependencySet(transaction, EntryType(), EntrySchema(), EntryName());
		other_set.AddDependent(transaction, set, type);
	}
	case DependencyLinkSide::DEPENDENT: {
		auto &other_set = manager.GetOrCreateDependencySet(transaction, EntryType(), EntrySchema(), EntryName());
		other_set.AddDependency(transaction, set, type);
	}
	}
}

} // namespace duckdb
