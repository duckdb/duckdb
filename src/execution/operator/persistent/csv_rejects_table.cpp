#pragma once

#include "duckdb/main/appender.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/execution/operator/persistent/csv_rejects_table.hpp"

namespace duckdb {

TableCatalogEntry &CSVRejectsTable::GetTable(ClientContext &context, const string &name) {
	auto &temp_catalog = Catalog::GetCatalog(context, TEMP_CATALOG);
	auto &table_entry = temp_catalog.GetEntry<TableCatalogEntry>(context, TEMP_CATALOG, DEFAULT_SCHEMA, name);
	return table_entry;
}

shared_ptr<CSVRejectsTable> CSVRejectsTable::GetOrCreate(ClientContext &context) {
	const char *key = "CSV_REJECTS_TABLE_CACHE_ENTRY";
	auto &cache = ObjectCache::GetObjectCache(context);
	return cache.GetOrCreate<CSVRejectsTable>(key);
}

void CSVRejectsTable::InitializeTable(ClientContext &context, const ReadCSVData &data, const string &name) {
	// (Re)Create the temporary rejects table
	auto &catalog = Catalog::GetCatalog(context, TEMP_CATALOG);
	auto info = make_uniq<CreateTableInfo>(TEMP_CATALOG, DEFAULT_SCHEMA, name);
	info->temporary = true;
	info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
	info->columns.AddColumn(ColumnDefinition("line", LogicalType::BIGINT));
	info->columns.AddColumn(ColumnDefinition("column", LogicalType::BIGINT));
	info->columns.AddColumn(ColumnDefinition("column_name", LogicalType::VARCHAR));
	info->columns.AddColumn(ColumnDefinition("parsed_value", LogicalType::VARCHAR));

	if (!data.options.rejects_recovery_columns.empty()) {
		child_list_t<LogicalType> recovery_key_components;
		for (auto &key_idx : data.options.rejects_recovery_columns) {
			auto &col_name = data.csv_names[key_idx];
			auto &col_type = data.csv_types[key_idx];
			recovery_key_components.emplace_back(col_name, col_type);
		}
		info->columns.AddColumn(ColumnDefinition("recovery_columns", LogicalType::STRUCT(recovery_key_components)));
	}

	info->columns.AddColumn(ColumnDefinition("error", LogicalType::VARCHAR));
	info->columns.AddColumn(ColumnDefinition("file", LogicalType::VARCHAR));

	catalog.CreateTable(context, std::move(info));

	count = 0;
}

} // namespace duckdb
