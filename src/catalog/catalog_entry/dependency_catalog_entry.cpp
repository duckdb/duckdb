#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/dependency_manager.hpp"

namespace duckdb {

DependencyCatalogEntry::DependencyCatalogEntry(Catalog &catalog, const CatalogEntryInfo &info,
                                               const CatalogEntryInfo &from, DependencyFlags flags)
    : InCatalogEntry(
          CatalogType::DEPENDENCY_ENTRY, catalog,
          MangledDependencyName(DependencyManager::MangleName(from), DependencyManager::MangleName(info)).name),
      mangled_name(DependencyManager::MangleName(info)), entry(info),
      from_mangled_name(DependencyManager::MangleName(from)), from(from), flags(flags) {
	D_ASSERT(info.type != CatalogType::DEPENDENCY_ENTRY);
}

const MangledEntryName &DependencyCatalogEntry::MangledName() const {
	return mangled_name;
}

CatalogType DependencyCatalogEntry::EntryType() const {
	return entry.type;
}

const string &DependencyCatalogEntry::EntrySchema() const {
	return entry.schema;
}

const string &DependencyCatalogEntry::EntryName() const {
	return entry.name;
}

const CatalogEntryInfo &DependencyCatalogEntry::EntryInfo() const {
	return entry;
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

const CatalogEntryInfo &DependencyCatalogEntry::FromInfo() const {
	return from;
}

const DependencyFlags &DependencyCatalogEntry::Flags() const {
	return flags;
}

DependencyCatalogEntry::~DependencyCatalogEntry() {
}

} // namespace duckdb
