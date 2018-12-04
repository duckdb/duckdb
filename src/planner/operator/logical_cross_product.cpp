
#include "planner/operator/logical_cross_product.hpp"

using namespace duckdb;
using namespace std;

vector<string> LogicalCrossProduct::GetNames() {
	auto left = children[0]->GetNames();
	auto right = children[1]->GetNames();
	left.insert(left.end(), right.begin(), right.end());
	return left;
}
