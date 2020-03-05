#include "duckdb/planner/operator/logical_table_function.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

using namespace duckdb;
using namespace std;

vector<ColumnBinding> LogicalTableFunction::GetColumnBindings() {
	return GenerateColumnBindings(table_index, return_types.size());
}

void LogicalTableFunction::ResolveTypes() {
	for (auto &type : return_types) {
		types.push_back(GetInternalType(type));
	}
}
