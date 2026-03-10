#include "duckdb/planner/operator/logical_pivot.hpp"

#include "duckdb/main/config.hpp"

namespace duckdb {

LogicalPivot::LogicalPivot() : LogicalOperator(LogicalOperatorType::LOGICAL_PIVOT) {
}

LogicalPivot::LogicalPivot(TableIndex pivot_idx, unique_ptr<LogicalOperator> plan, BoundPivotInfo info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_PIVOT), pivot_index(pivot_idx), bound_pivot(std::move(info_p)) {
	D_ASSERT(plan);
	children.push_back(std::move(plan));
}

vector<ColumnBinding> LogicalPivot::GetColumnBindings() {
	vector<ColumnBinding> result;
	for (idx_t i = 0; i < bound_pivot.types.size(); i++) {
		result.emplace_back(pivot_index, i);
	}
	return result;
}

vector<TableIndex> LogicalPivot::GetTableIndex() const {
	return vector<TableIndex> {pivot_index};
}

void LogicalPivot::ResolveTypes() {
	this->types = bound_pivot.types;
}

string LogicalPivot::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", pivot_index.index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
