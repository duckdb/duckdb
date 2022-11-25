#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/common/field_writer.hpp"

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
	writer.WriteSerializableList<Expression>(expressions);
}

unique_ptr<LogicalOperator> LogicalWindow::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto window_index = reader.ReadRequired<idx_t>();
	auto result = make_unique<LogicalWindow>(window_index);
	result->expressions = reader.ReadRequiredSerializableList<Expression>(state.gstate);
	return move(result);
}

vector<idx_t> LogicalWindow::GetTableIndex() const {
	return vector<idx_t> {window_index};
}

} // namespace duckdb
