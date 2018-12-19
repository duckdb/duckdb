#include "planner/operator/logical_window.hpp"

using namespace duckdb;
using namespace std;

vector<string> LogicalWindow::GetNames() {
	auto names = children[0]->GetNames();
	for (auto &exp : expressions) {
		names.push_back(exp->GetName());
	}
	return names;
}

void LogicalWindow::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}
