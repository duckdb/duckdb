
#include "planner/operator/logical_cross_product.hpp"

using namespace duckdb;
using namespace std;

vector<string> LogicalCrossProduct::GetNames() {
	auto left = children[0]->GetNames();
	auto right = children[1]->GetNames();
	left.insert(left.end(), right.begin(), right.end());
	return left;
}

void LogicalCrossProduct::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	types.insert(types.end(), children[1]->types.begin(), children[1]->types.end());
}
