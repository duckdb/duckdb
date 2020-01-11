#include "duckdb/planner/operator/logical_prune_columns.hpp"

using namespace duckdb;
using namespace std;

vector<ColumnBinding> LogicalPruneColumns::GetColumnBindings() {
	vector<ColumnBinding> result;
	auto child_bindings = children[0]->GetColumnBindings();
	result.insert(result.end(), child_bindings.begin(), child_bindings.begin() + column_limit);
	return result;
}

void LogicalPruneColumns::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.begin() + column_limit);
}
