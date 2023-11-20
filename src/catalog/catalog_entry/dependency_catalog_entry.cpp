#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency_set_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/dependency_manager.hpp"

namespace duckdb {

DependencyCatalogEntry::DependencyCatalogEntry(DependencyLinkSide side, Catalog &catalog, DependencyManager &manager,
                                               CatalogType entry_type, const string &entry_schema,
                                               const string &entry_name, DependencyType dependency_type)
    : InCatalogEntry(CatalogType::DEPENDENCY_ENTRY, catalog,
                     DependencyManager::MangleName(entry_type, entry_schema, entry_name).name),
      mangled_name(DependencyManager::MangleName(entry_type, entry_schema, entry_name)), entry_name(entry_name),
      entry_schema(entry_schema), entry_type(entry_type), dependency_type(dependency_type), side(side),
      manager(manager) {
	D_ASSERT(entry_type != CatalogType::DEPENDENCY_ENTRY);
}

const MangledEntryName &DependencyCatalogEntry::MangledName() const {
	return mangled_name;
}

CatalogType DependencyCatalogEntry::EntryType() const {
	return entry_type;
}

const string &DependencyCatalogEntry::EntrySchema() const {
	return entry_schema;
}

const string &DependencyCatalogEntry::EntryName() const {
	return entry_name;
}

const MangledEntryName &DependencyCatalogEntry::FromMangledName() const {
	return from_mangled_name;
}

CatalogType DependencyCatalogEntry::FromType() const {
	return from.type;
}

const string &DependencyCatalogEntry::FromSchema() const {
	return from.schema;
}

const string &DependencyCatalogEntry::FromName() const {
	return from.name;
}

DependencyType DependencyCatalogEntry::Type() const {
	return dependency_type;
}

void DependencyCatalogEntry::SetFrom(const MangledEntryName &from_mangled_name, const CatalogEntryInfo &info,
                                     const string &new_name) {
	name = new_name;
	this->from_mangled_name = from_mangled_name;
	this->from = info;
}

DependencyCatalogEntry::~DependencyCatalogEntry() {
}

void DependencyCatalogEntry::CompleteLink(CatalogTransaction transaction, DependencyType type) {
	auto set = manager.GetDependencySet(transaction, FromType(), FromSchema(), FromName());
	switch (side) {
	case DependencyLinkSide::DEPENDENCY: {
		auto other_set = manager.GetDependencySet(transaction, EntryType(), EntrySchema(), EntryName());
		other_set.AddDependent(transaction, set, type);
		break;
	}
	case DependencyLinkSide::DEPENDENT: {
		auto other_set = manager.GetDependencySet(transaction, EntryType(), EntrySchema(), EntryName());
		other_set.AddDependency(transaction, set, type);
		break;
	}
	}
}

} // namespace duckdb
