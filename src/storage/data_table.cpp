
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


vector<TypeId> DataTable::GetTypes(const vector<size_t>& column_ids) {
	vector<TypeId> types;
	for (auto &column_id : column_ids) {
		types.push_back(columns[column_id]->column.type);
	}
	return types;
}

void DataTable::Scan(Transaction &transaction, DataChunk &result, const vector<size_t>& column_ids, size_t &offset) {
	for (size_t i = 0; i < column_ids.size(); i++) {
		auto *column = columns[column_ids[i]].get();
		if (offset >= column->data.size())
			return;
		auto &v = column->data[offset];
		result.data[i].Reference(*v);
	}
	result.count = result.data[0].count;
	offset++;
}
