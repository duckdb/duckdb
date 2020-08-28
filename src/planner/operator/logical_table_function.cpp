#include "duckdb/planner/operator/logical_table_function.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

namespace duckdb {
using namespace std;

vector<ColumnBinding> LogicalTableFunction::GetColumnBindings() {
	vector<ColumnBinding> result;
	for (idx_t i = 0; i < column_ids.size(); i++) {
		result.push_back(ColumnBinding(table_index, i));
	}
	return result;
}

void LogicalTableFunction::ResolveTypes() {
	for (auto col_idx : column_ids) {
		if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
			types.push_back(LOGICAL_ROW_TYPE);
			continue;
		}
		types.push_back(return_types[col_idx]);
	}
}

string LogicalTableFunction::ParamsToString() const {
	return "(" + function.name + ")";
}

} // namespace duckdb
