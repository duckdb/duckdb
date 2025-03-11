#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"

#include "duckdb/storage/data_table.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"

namespace duckdb {

IndexDataTableInfo::IndexDataTableInfo(shared_ptr<DataTableInfo> info_p, const string &index_name_p)
    : info(std::move(info_p)), index_name(index_name_p) {
}

void DuckIndexEntry::Rollback(CatalogEntry &) {
	if (!info) {
		return;
	}
	if (!info->info) {
		return;
	}
	info->info->GetIndexes().RemoveIndex(name);
}

DuckIndexEntry::DuckIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &create_info,
                               TableCatalogEntry &table_p)
    : IndexCatalogEntry(catalog, schema, create_info), initial_index_size(0) {

	auto &table = table_p.Cast<DuckTableEntry>();
	auto &storage = table.GetStorage();
	info = make_shared_ptr<IndexDataTableInfo>(storage.GetDataTableInfo(), name);
}

DuckIndexEntry::DuckIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &create_info,
                               shared_ptr<IndexDataTableInfo> storage_info)
    : IndexCatalogEntry(catalog, schema, create_info), info(std::move(storage_info)), initial_index_size(0) {
}

unique_ptr<CatalogEntry> DuckIndexEntry::Copy(ClientContext &context) const {
	auto info_copy = GetInfo();
	auto &cast_info = info_copy->Cast<CreateIndexInfo>();

	auto result = make_uniq<DuckIndexEntry>(catalog, schema, cast_info, info);
	result->initial_index_size = initial_index_size;

	return std::move(result);
}

string DuckIndexEntry::GetSchemaName() const {
	return GetDataTableInfo().GetSchemaName();
}

string DuckIndexEntry::GetTableName() const {
	return GetDataTableInfo().GetTableName();
}

DataTableInfo &DuckIndexEntry::GetDataTableInfo() const {
	return *info->info;
}

void DuckIndexEntry::CommitDrop() {
	D_ASSERT(info);
	auto &indexes = GetDataTableInfo().GetIndexes();
	indexes.CommitDrop(name);
	indexes.RemoveIndex(name);
}

} // namespace duckdb
