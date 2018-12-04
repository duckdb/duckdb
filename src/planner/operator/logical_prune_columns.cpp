

#include "planner/operator/logical_prune_columns.hpp"

using namespace duckdb;
using namespace std;

vector<string> LogicalPruneColumns::GetNames() {
	auto names = children[0]->GetNames();
	assert(column_limit <= names.size());
	names.erase(names.begin() + column_limit, names.end());
	return names;
}
