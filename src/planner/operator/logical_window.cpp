#include "planner/operator/logical_window.hpp"

using namespace duckdb;
using namespace std;

vector<string> LogicalWindow::GetNames() {
	vector<string> names;
	for (auto &exp : expressions) {
		names.push_back(exp->GetName());
	}
	return names;
}

void LogicalWindow::ResolveTypes() {
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}
