#include "duckdb/main/appender.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/execution/operator/persistent/csv_rejects_table.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"

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

	// Create CSV_ERROR_TYPE ENUM
	string enum_name = "CSV_ERROR_TYPE";
	Vector order_errors(LogicalType::VARCHAR, 6);
	order_errors.SetValue(0, "CAST");
	order_errors.SetValue(1, "MISSING COLUMNS");
	order_errors.SetValue(2, "TOO MANY COLUMNS");
	order_errors.SetValue(3, "UNQUOTED VALUE");
	order_errors.SetValue(4, "LINE SIZE OVER MAXIMUM");
	order_errors.SetValue(5, "INVALID UNICODE");
	LogicalType enum_type = LogicalType::ENUM(enum_name, order_errors, 6);
	auto type_info = make_uniq<CreateTypeInfo>(enum_name, enum_type);
	type_info->temporary = true;
	type_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	catalog.CreateType(context, *type_info);

	// Create Rejects Table
	auto info = make_uniq<CreateTableInfo>(TEMP_CATALOG, DEFAULT_SCHEMA, name);
	info->temporary = true;
	info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
	// 1. File Path
	info->columns.AddColumn(ColumnDefinition("file", LogicalType::VARCHAR));
	// 2. Row Line
	info->columns.AddColumn(ColumnDefinition("line", LogicalType::UBIGINT));
	// 3. Byte Position where error occurred
	info->columns.AddColumn(ColumnDefinition("byte_position", LogicalType::UBIGINT));
	// 4. Column Index (If Applicable)
	info->columns.AddColumn(ColumnDefinition("column_idx", LogicalType::UBIGINT));
	// 5. Column Name (If Applicable)
	info->columns.AddColumn(ColumnDefinition("column_name", LogicalType::VARCHAR));
	// 6. Error Type
	info->columns.AddColumn(ColumnDefinition("error_type", enum_type));
	// 7. Original CSV Line
	info->columns.AddColumn(ColumnDefinition("csv_line", LogicalType::VARCHAR));
	// 8. Full Error Message
	info->columns.AddColumn(ColumnDefinition("error_message", LogicalType::VARCHAR));
	catalog.CreateTable(context, std::move(info));
	count = 0;
}

} // namespace duckdb
