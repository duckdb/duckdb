#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"

namespace duckdb {

DependencyCatalogEntry::DependencyCatalogEntry(Catalog &catalog, const string &name)
    : InCatalogEntry(CatalogType::DEPENDENCY_ENTRY, catalog, name) {
}

DependencyCatalogEntry::~DependencyCatalogEntry() {
}

} // namespace duckdb
