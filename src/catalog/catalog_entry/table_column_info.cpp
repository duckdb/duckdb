#include "duckdb/catalog/catalog_entry/table_column_info.hpp"

namespace duckdb {

TableColumnInfo::TableColumnInfo(column_t index, TableColumnType column_type) : index(index), column_type(column_type) {
}

TableColumnInfo::TableColumnInfo(const TableColumnInfo &table_column_info)
    : index(table_column_info.index), column_type(table_column_info.column_type) {
}

TableColumnInfo &TableColumnInfo::operator=(const TableColumnInfo &table_column_info) {
	this->column_type = table_column_info.column_type;
	this->index = table_column_info.index;
	return *this;
}

} // namespace duckdb
