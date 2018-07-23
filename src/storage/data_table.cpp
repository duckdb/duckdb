
#include "common/helper.hpp"

#include "storage/data_table.hpp"
#include "storage/data_column.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

void DataTable::AddColumn(ColumnCatalogEntry& column) {
	columns.push_back(make_unique<DataColumn>(*this, column));
}

void DataTable::AddData(DataChunk& chunk) {
	if (chunk.colcount != columns.size()) {
		throw CatalogException("Mismatch in column count for append");
	}
	for (size_t i = 0; i < chunk.colcount; i++) {
		columns[i]->AddData(*chunk.data[i].get());
	}
	size += chunk.count;
}

