#include "planner/operator/logical_projection.hpp"

using namespace duckdb;
using namespace std;

vector<string> LogicalProjection::GetNames() {
	vector<string> names;
	for (auto &exp : expressions) {
		names.push_back(exp->GetName());
	}
	return names;
}

void LogicalProjection::ResolveTypes() {
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}
