#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

DuckIndexEntry::DuckIndexEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info)
    : IndexCatalogEntry(catalog, schema, info) {
}

DuckIndexEntry::~DuckIndexEntry() {
	// remove the associated index from the info
	if (!info || !index) {
		return;
	}
	info->indexes.RemoveIndex(index);
}

string DuckIndexEntry::GetSchemaName() {
	return info->schema;
}

string DuckIndexEntry::GetTableName() {
	return info->table;
}

} // namespace duckdb
