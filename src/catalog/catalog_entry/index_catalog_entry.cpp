#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

IndexCatalogEntry::IndexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info)
    : StandardEntry(CatalogType::INDEX_ENTRY, schema, catalog, info->index_name), index(nullptr), sql(info->sql) {
}

IndexCatalogEntry::~IndexCatalogEntry() {
	// remove the associated index from the info
	if (!info || !index) {
		return;
	}
	info->indexes.RemoveIndex(index);
}

string IndexCatalogEntry::ToSQL() {
	if (sql.empty()) {
		throw InternalException("Cannot convert INDEX to SQL because it was not created with a SQL statement");
	}
	return sql;
}

idx_t IndexCatalogEntry::Serialize(duckdb::MetaBlockWriter &writer) {
	if (index->type != IndexType::ART) {
		throw NotImplementedException("The implementation of this index serialization does not exist.");
	}
	// We first do a DFS on the ART
	auto art_index = (ART *)index;
	return art_index->DepthFirstSearchCheckpoint(writer);
}

} // namespace duckdb
