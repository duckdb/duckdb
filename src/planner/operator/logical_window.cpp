#include "duckdb/planner/operator/logical_window.hpp"

using namespace duckdb;
using namespace std;

void LogicalWindow::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}
