#include "duckdb/planner/operator/logical_get.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {
using namespace std;

LogicalGet::LogicalGet(idx_t table_index, TableFunction function, unique_ptr<FunctionData> bind_data, vector<LogicalType> returned_types, vector<string> returned_names) :
	LogicalOperator(LogicalOperatorType::GET), table_index(table_index), function(move(function)), bind_data(move(bind_data)), returned_types(move(returned_types)), names(move(names)) {}

string LogicalGet::ParamsToString() const {
	return string();
	// return "(" + table->name + ")";
}

vector<ColumnBinding> LogicalGet::GetColumnBindings() {
	if (column_ids.size() == 0) {
		return {ColumnBinding(table_index, 0)};
	}
	vector<ColumnBinding> result;
	for (idx_t i = 0; i < column_ids.size(); i++) {
		result.push_back(ColumnBinding(table_index, i));
	}
	return result;
}

void LogicalGet::ResolveTypes() {
	if (column_ids.size() == 0) {
		column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	}
	for (auto &index : column_ids) {
		if (index == COLUMN_IDENTIFIER_ROW_ID) {
			types.push_back(LOGICAL_ROW_TYPE);
		} else {
			types.push_back(returned_types[index]);
		}
	}
}

idx_t LogicalGet::EstimateCardinality() {
	if (function.cardinality) {
		return function.cardinality(bind_data.get());
	} else {
		return 1;
	}
}

} // namespace duckdb
