#include "duckdb/planner/operator/logical_get.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"

using namespace duckdb;
using namespace std;

LogicalGet::LogicalGet(index_t table_index)
    : LogicalOperator(LogicalOperatorType::GET), table(nullptr), table_index(table_index) {
}
LogicalGet::LogicalGet(TableCatalogEntry *table, index_t table_index, vector<column_t> column_ids)
    : LogicalOperator(LogicalOperatorType::GET), table(table), table_index(table_index), column_ids(column_ids) {
}

string LogicalGet::ParamsToString() const {
	if (!table) {
		return "";
	}
	return "(" + table->name + ")";
}

vector<ColumnBinding> LogicalGet::GetColumnBindings() {
	if (!table) {
		return {ColumnBinding(INVALID_INDEX, 0)};
	}
	vector<ColumnBinding> result;
	for (index_t i = 0; i < column_ids.size(); i++) {
		result.push_back(ColumnBinding(table_index, i));
	}
	return result;
}

void LogicalGet::ResolveTypes() {
	if (column_ids.size() == 0) {
		types = {TypeId::INT32};
	} else {
		types = table->GetTypes(column_ids);
	}
}

index_t LogicalGet::EstimateCardinality() {
	if (table) {
		return table->storage->cardinality;
	} else {
		return 1;
	}
}
