#include "duckdb/planner/operator/logical_row_presence.hpp"

namespace duckdb {

LogicalRowPresence::LogicalRowPresence(TableIndex presence_index)
    : LogicalOperator(LogicalOperatorType::LOGICAL_ROW_PRESENCE), presence_index(presence_index) {
}

LogicalRowPresence::LogicalRowPresence(TableIndex presence_index, unique_ptr<LogicalOperator> child)
    : LogicalRowPresence(presence_index) {
	children.push_back(std::move(child));
}

vector<ColumnBinding> LogicalRowPresence::GetColumnBindings() {
	auto child_bindings = children[0]->GetColumnBindings();
	child_bindings.emplace_back(presence_index, ProjectionIndex(0));
	return child_bindings;
}

vector<TableIndex> LogicalRowPresence::GetTableIndex() const {
	return vector<TableIndex> {presence_index};
}

void LogicalRowPresence::ResolveTypes() {
	types = children[0]->types;
	types.push_back(LogicalType::BOOLEAN);
}

} // namespace duckdb
