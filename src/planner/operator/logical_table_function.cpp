#include "planner/operator/logical_table_function.hpp"

#include "catalog/catalog_entry/table_function_catalog_entry.hpp"

using namespace duckdb;
using namespace std;

void LogicalTableFunction::ResolveTypes() {
	for (auto &column : function->return_values) {
		types.push_back(GetInternalType(column.type));
	}
}
