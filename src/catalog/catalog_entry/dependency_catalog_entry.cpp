#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

static string GetSchema(CatalogEntry &entry) {
	if (entry.type == CatalogType::SCHEMA_ENTRY) {
		return entry.name;
	}
	return entry.ParentSchema().name;
}

DependencyCatalogEntry::DependencyCatalogEntry(DependencyConnectionType type, Catalog &catalog, CatalogEntry &entry,
                                               DependencyType dependency_type)
    : InCatalogEntry(CatalogType::DEPENDENCY_ENTRY, catalog, entry.name), connection_type(type), name(entry.name),
      schema(GetSchema(entry)), entry_type(entry.type), dependency_type(dependency_type) {
}

DependencyCatalogEntry::~DependencyCatalogEntry() {
}

} // namespace duckdb
