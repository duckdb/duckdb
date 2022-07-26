#include "duckdb/planner/operator/logical_window.hpp"

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

void LogicalWindow::Serialize(FieldWriter &writer) const {
	writer.WriteField(window_index);
}

unique_ptr<LogicalOperator> LogicalWindow::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                       FieldReader &reader) {
	auto window_index = reader.ReadRequired<idx_t>();
	return make_unique<LogicalWindow>(window_index);
}

} // namespace duckdb
