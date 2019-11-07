#include "duckdb/planner/operator/logical_prune_columns.hpp"

using namespace duckdb;
using namespace std;

void LogicalPruneColumns::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.begin() + column_limit);
}
