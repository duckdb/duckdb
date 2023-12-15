#include "duckdb/planner/operator/logical_window.hpp"

#include "duckdb/main/config.hpp"

namespace duckdb {

vector<ColumnBinding> LogicalWindow::GetColumnBindings() {
	auto child_bindings = children[0]->GetColumnBindings();
	for (idx_t i = 0; i < expressions.size(); i++) {
		child_bindings.emplace_back(window_index, i);
	}
	return child_bindings;
}

void LogicalWindow::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}

vector<idx_t> LogicalWindow::GetTableIndex() const {
	return vector<idx_t> {window_index};
}

string LogicalWindow::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", window_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
