#include "planner/operator/logical_table_function.hpp"

using namespace duckdb;
using namespace std;

void LogicalTableFunction::ResolveTypes() {
	for (auto &column : function->return_values) {
		types.push_back(GetInternalType(column.type));
	}
}
