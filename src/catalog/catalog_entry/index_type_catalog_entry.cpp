#include "duckdb/catalog/catalog_entry/index_type_catalog_entry.hpp"

namespace duckdb {

IndexTypeCatalogEntry::IndexTypeCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexTypeInfo &info)
    : StandardEntry(CatalogType::INDEX_TYPE_ENTRY, schema, catalog, info.index_type_name) {
	this->temporary = info.temporary;
	this->internal = info.internal;
	this->create_instance = nullptr;
}

} // namespace duckdb
