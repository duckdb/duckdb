#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency_set_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/dependency/dependency_manager.hpp"

namespace duckdb {

DependencyCatalogEntry::DependencyCatalogEntry(DependencyLinkSide side, Catalog &catalog, DependencyManager &manager,
                                               CatalogType entry_type, const string &entry_schema,
                                               const string &entry_name, DependencyType dependency_type)
    : InCatalogEntry(CatalogType::DEPENDENCY_ENTRY, catalog,
                     DependencyManager::MangleName(entry_type, entry_schema, entry_name)),
      mangled_name(name), entry_name(entry_name), entry_schema(entry_schema), entry_type(entry_type),
      dependency_type(dependency_type), side(side), manager(manager) {
	D_ASSERT(entry_type != CatalogType::DEPENDENCY_ENTRY);
}

const string &DependencyCatalogEntry::MangledName() const {
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

const string &DependencyCatalogEntry::FromMangledName() const {
	return from_mangled_name;
}

CatalogType DependencyCatalogEntry::FromType() const {
	return from_type;
}

const string &DependencyCatalogEntry::FromSchema() const {
	return from_schema;
}

const string &DependencyCatalogEntry::FromName() const {
	return from_name;
}

DependencyType DependencyCatalogEntry::Type() const {
	return dependency_type;
}

void DependencyCatalogEntry::SetFrom(const string &from_mangled_name, CatalogType from_type, const string &from_schema,
                                     const string &from_name, const string &new_name) {
	name = new_name;
	this->from_mangled_name = from_mangled_name;
	this->from_type = from_type;
	this->from_schema = from_schema;
	this->from_name = from_name;
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
