#include "duckdb/catalog/catalog_entry/dindex_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

DIndexCatalogEntry::DIndexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info)
    : IndexCatalogEntry(catalog, schema, info) {
}

DIndexCatalogEntry::~DIndexCatalogEntry() {
	// remove the associated index from the info
	if (!info || !index) {
		return;
	}
	info->indexes.RemoveIndex(index);
}

string DIndexCatalogEntry::GetSchemaName() {
	return info->schema;
}

string DIndexCatalogEntry::GetTableName() {
	return info->table;
}

} // namespace duckdb
