#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"

#include "duckdb/storage/data_table.hpp"

namespace duckdb {

IndexDataTableInfo::IndexDataTableInfo(shared_ptr<DataTableInfo> &info_p, const string &index_name_p)
    : info(info_p), index_name(index_name_p) {
}

IndexDataTableInfo::~IndexDataTableInfo() {
	if (!info) {
		return;
	}
	info->indexes.RemoveIndex(index_name);
}

DuckIndexEntry::DuckIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info)
    : IndexCatalogEntry(catalog, schema, info) {
}

unique_ptr<CatalogEntry> DuckIndexEntry::Copy(ClientContext &context) const {
	auto info_copy = GetInfo();
	auto &cast_info = info_copy->Cast<CreateIndexInfo>();

	auto result = make_uniq<DuckIndexEntry>(catalog, schema, cast_info);
	result->info = info;
	result->initial_index_size = initial_index_size;

	for (auto &expr : expressions) {
		result->expressions.push_back(expr->Copy());
	}
	for (auto &expr : parsed_expressions) {
		result->parsed_expressions.push_back(expr->Copy());
	}

	return std::move(result);
}

string DuckIndexEntry::GetSchemaName() const {
	return GetDataTableInfo().schema;
}

string DuckIndexEntry::GetTableName() const {
	return GetDataTableInfo().table;
}

DataTableInfo &DuckIndexEntry::GetDataTableInfo() const {
	return *info->info;
}

void DuckIndexEntry::CommitDrop() {
	D_ASSERT(info);
	GetDataTableInfo().indexes.CommitDrop(name);
}

} // namespace duckdb
