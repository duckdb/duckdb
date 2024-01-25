#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"

#include "duckdb/storage/data_table.hpp"

namespace duckdb {

DuckIndexEntry::DuckIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info)
    : IndexCatalogEntry(catalog, schema, info) {
}

DuckIndexEntry::~DuckIndexEntry() {
	if (!info) {
		return;
	}
	info->indexes.RemoveIndex(name);
}

string DuckIndexEntry::GetSchemaName() const {
	return info->schema;
}

string DuckIndexEntry::GetTableName() const {
	return info->table;
}

void DuckIndexEntry::CommitDrop() {
	D_ASSERT(info);
	info->indexes.CommitDrop(name);
}

} // namespace duckdb
