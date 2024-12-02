#include "duckdb/parser/parsed_data/exported_table_data.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

ExportedTableInfo::ExportedTableInfo(TableCatalogEntry &entry, ExportedTableData table_data_p,
                                     vector<string> &not_null_columns_p)
    : entry(entry), table_data(std::move(table_data_p)) {
	table_data.not_null_columns = not_null_columns_p;
}

ExportedTableInfo::ExportedTableInfo(ClientContext &context, ExportedTableData table_data_p)
    : entry(GetEntry(context, table_data_p)), table_data(std::move(table_data_p)) {
}

TableCatalogEntry &ExportedTableInfo::GetEntry(ClientContext &context, const ExportedTableData &table_data) {
	return Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, table_data.database_name, table_data.schema_name,
	                         table_data.table_name)
	    .Cast<TableCatalogEntry>();
}

} // namespace duckdb
