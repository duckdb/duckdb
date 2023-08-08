#include "duckdb/main/appender.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/execution/operator/persistent/csv_rejects_table.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

TableCatalogEntry &CSVRejectsTable::GetTable(ClientContext &context) {
	auto &temp_catalog = Catalog::GetCatalog(context, TEMP_CATALOG);
	auto &table_entry = temp_catalog.GetEntry<TableCatalogEntry>(context, TEMP_CATALOG, DEFAULT_SCHEMA, name);
	return table_entry;
}

shared_ptr<CSVRejectsTable> CSVRejectsTable::GetOrCreate(ClientContext &context, const string &name) {
	auto key = "CSV_REJECTS_TABLE_CACHE_ENTRY_" + StringUtil::Upper(name);
	auto &cache = ObjectCache::GetObjectCache(context);
	return cache.GetOrCreate<CSVRejectsTable>(key, name);
}

void CSVRejectsTable::InitializeTable(ClientContext &context, const ReadCSVData &data) {
	// (Re)Create the temporary rejects table
	auto &catalog = Catalog::GetCatalog(context, TEMP_CATALOG);
	auto info = make_uniq<CreateTableInfo>(TEMP_CATALOG, DEFAULT_SCHEMA, name);
	info->temporary = true;
	info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
	info->columns.AddColumn(ColumnDefinition("file", LogicalType::VARCHAR));
	info->columns.AddColumn(ColumnDefinition("line", LogicalType::BIGINT));
	info->columns.AddColumn(ColumnDefinition("column", LogicalType::BIGINT));
	info->columns.AddColumn(ColumnDefinition("column_name", LogicalType::VARCHAR));
	info->columns.AddColumn(ColumnDefinition("parsed_value", LogicalType::VARCHAR));

	if (!data.options.rejects_recovery_columns.empty()) {
		child_list_t<LogicalType> recovery_key_components;
		for (auto &col_name : data.options.rejects_recovery_columns) {
			recovery_key_components.emplace_back(col_name, LogicalType::VARCHAR);
		}
		info->columns.AddColumn(ColumnDefinition("recovery_columns", LogicalType::STRUCT(recovery_key_components)));
	}

	info->columns.AddColumn(ColumnDefinition("error", LogicalType::VARCHAR));

	catalog.CreateTable(context, std::move(info));

	count = 0;
}

} // namespace duckdb
