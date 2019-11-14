#include "duckdb/planner/operator/logical_cross_product.hpp"

using namespace duckdb;
using namespace std;

void LogicalCrossProduct::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	types.insert(types.end(), children[1]->types.begin(), children[1]->types.end());
}
