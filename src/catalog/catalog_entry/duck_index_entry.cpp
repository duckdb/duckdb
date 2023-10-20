#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

DuckIndexEntry::DuckIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info)
    : IndexCatalogEntry(catalog, schema, info) {
}

DuckIndexEntry::~DuckIndexEntry() {
	// remove the associated index from the info
	if (!info || !index) {
		return;
	}
	info->indexes.RemoveIndex(*index);
}

string DuckIndexEntry::GetSchemaName() const {
	return info->schema;
}

string DuckIndexEntry::GetTableName() const {
	return info->table;
}

void DuckIndexEntry::CommitDrop() {
	D_ASSERT(info && index);
	index->CommitDrop();
}

} // namespace duckdb
