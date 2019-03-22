#include "planner/operator/logical_table_function.hpp"

using namespace duckdb;
using namespace std;

vector<string> LogicalTableFunction::GetNames() {
	vector<string> names;
	for (auto &column : function->return_values) {
		names.push_back(column.name);
	}
	return names;
}

void LogicalTableFunction::ResolveTypes() {
	for (auto &column : function->return_values) {
		types.push_back(GetInternalType(column.type));
	}
}
