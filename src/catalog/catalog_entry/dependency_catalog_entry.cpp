#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/dependency_manager.hpp"

namespace duckdb {

static string GetSchema(CatalogEntry &entry) {
	if (entry.type == CatalogType::SCHEMA_ENTRY) {
		return entry.name;
	}
	return entry.ParentSchema().name;
}

DependencyCatalogEntry::DependencyCatalogEntry(Catalog &catalog, CatalogEntry &entry, DependencyType dependency_type)
    : InCatalogEntry(CatalogType::DEPENDENCY_ENTRY, catalog, DependencyManager::MangleName(entry)),
      entry_name(entry.name), schema(GetSchema(entry)), entry_type(entry.type), dependency_type(dependency_type) {
	D_ASSERT(entry.type != CatalogType::DEPENDENCY_ENTRY);
	D_ASSERT(entry.type != CatalogType::DEPENDENCY_SET);
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

} // namespace duckdb
