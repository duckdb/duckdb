
#include "common/helper.hpp"

#include "storage/data_column.hpp"
#include "storage/data_table.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

void DataTable::AddColumn(ColumnDefinition &column) {
	columns.push_back(make_unique<DataColumn>(*this, column));
}

void DataTable::AddData(DataChunk &chunk) {
	if (chunk.count == 0) {
		return;
	}
	if (chunk.column_count != columns.size()) {
		throw CatalogException("Mismatch in column count for append");
	}
	for (size_t i = 0; i < chunk.column_count; i++) {
		columns[i]->AddData(chunk.data[i]);
	}
	size += chunk.count;
}
