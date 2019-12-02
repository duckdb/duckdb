#include "duckdb/planner/operator/logical_projection.hpp"

using namespace duckdb;
using namespace std;

void LogicalProjection::ResolveTypes() {
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}
