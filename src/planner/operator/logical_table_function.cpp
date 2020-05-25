#include "duckdb/planner/operator/logical_table_function.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

using namespace duckdb;
using namespace std;

vector<ColumnBinding> LogicalTableFunction::GetColumnBindings() {
	return GenerateColumnBindings(table_index, return_types.size());
}

void LogicalTableFunction::ResolveTypes() {
	if (column_ids.size() == 0) {
		column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	}
	for (auto &type : return_types) {
		types.push_back(GetInternalType(type));
	}
}

string LogicalTableFunction::ParamsToString() const {
	return "(" + function->name + ")";
}
