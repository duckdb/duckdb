#include "duckdb/planner/operator/logical_unnest.hpp"

#include <vector>

#include "duckdb/common/projection_index.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

vector<ColumnBinding> LogicalUnnest::GetColumnBindings() {
	auto child_bindings = children[0]->GetColumnBindings();
	for (auto unnest_col_idx : ProjectionIndex::GetIndexes(expressions.size())) {
		child_bindings.emplace_back(unnest_index, unnest_col_idx);
	}
	return child_bindings;
}

void LogicalUnnest::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}

vector<TableIndex> LogicalUnnest::GetTableIndex() const {
	return vector<TableIndex> {unnest_index};
}

string LogicalUnnest::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", unnest_index.index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
