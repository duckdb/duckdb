#include "duckdb/planner/operator/logical_table_function.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

using namespace duckdb;
using namespace std;

void LogicalTableFunction::ResolveTypes() {
	for (auto &type : function->function.types) {
		types.push_back(GetInternalType(type));
	}
}
