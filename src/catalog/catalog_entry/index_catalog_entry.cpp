#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {


IndexCatalogEntry::IndexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info)
	: StandardEntry(CatalogType::INDEX_ENTRY, schema, catalog, info->index_name), index(nullptr), sql(info->sql) {
}

IndexCatalogEntry::~IndexCatalogEntry() {
	// remove the associated index from the info
	if (!info || !index) {
		return;
	}
	for (idx_t i = 0; i < info->indexes.size(); i++) {
		if (info->indexes[i].get() == index) {
			info->indexes.erase(info->indexes.begin() + i);
			break;
		}
	}
}

string IndexCatalogEntry::ToSQL() {
	if (sql.size() == 0) {
		throw NotImplementedException("Cannot convert INDEX to SQL because it was not created with a SQL statement");
	}
	return sql;
}

} // namespace duckdb
