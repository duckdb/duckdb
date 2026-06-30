#include "duckdb/parser/parsed_data/exported_table_data.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

ExportedTableInfo::ExportedTableInfo(TableCatalogEntry &entry, ExportedTableData table_data_p,
                                     vector<Identifier> &not_null_columns_p)
    : entry(entry), table_data(std::move(table_data_p)) {
	table_data.not_null_columns = not_null_columns_p;
}

ExportedTableInfo::ExportedTableInfo(ClientContext &context, ExportedTableData table_data_p)
    : entry(GetEntry(context, table_data_p)), table_data(std::move(table_data_p)) {
}

TableCatalogEntry &ExportedTableInfo::GetEntry(ClientContext &context, const ExportedTableData &table_data) {
	return Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(table_data.qualified_name.Catalog(),
	                                                                   table_data.qualified_name.Schema(),
	                                                                   table_data.qualified_name.Name()));
}

} // namespace duckdb
