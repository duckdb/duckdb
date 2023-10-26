#include "duckdb/catalog/catalog_entry/dependency_set_catalog_entry.hpp"

namespace duckdb {

DependencySetCatalogEntry::DependencySetCatalogEntry(Catalog &catalog, const string &name)
    : InCatalogEntry(CatalogType::DEPENDENCY_SET, catalog, name), dependencies(catalog), dependents(catalog) {
}

CatalogSet &DependencySetCatalogEntry::Dependencies() {
	return dependencies;
}

CatalogSet &DependencySetCatalogEntry::Dependents() {
	return dependents;
}

DependencySetCatalogEntry::~DependencySetCatalogEntry() {
}

} // namespace duckdb
