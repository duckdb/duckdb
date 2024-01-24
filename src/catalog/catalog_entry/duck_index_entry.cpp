#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"

#include "duckdb/storage/data_table.hpp"

namespace duckdb {

DuckIndexEntry::DuckIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info)
    : IndexCatalogEntry(catalog, schema, info) {
}

unique_ptr<CatalogEntry> DuckIndexEntry::Copy(ClientContext &context) const {
	auto info_copy = GetInfo();
	auto &cast_info = info_copy->Cast<CreateIndexInfo>();

	auto result = make_uniq<DuckIndexEntry>(catalog, schema, cast_info);
	result->info = info;
	result->initial_index_size = initial_index_size;

	return std::move(result);
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
