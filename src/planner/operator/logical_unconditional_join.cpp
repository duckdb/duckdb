#include "duckdb/planner/operator/logical_unconditional_join.hpp"

namespace duckdb {

LogicalUnconditionalJoin::LogicalUnconditionalJoin(LogicalOperatorType logical_type, unique_ptr<LogicalOperator> left,
                                                   unique_ptr<LogicalOperator> right)
    : LogicalOperator(logical_type) {
	D_ASSERT(left);
	D_ASSERT(right);
	children.push_back(std::move(left));
	children.push_back(std::move(right));
}

vector<ColumnBinding> LogicalUnconditionalJoin::GetColumnBindings() {
	auto left_bindings = children[0]->GetColumnBindings();
	auto right_bindings = children[1]->GetColumnBindings();
	left_bindings.insert(left_bindings.end(), right_bindings.begin(), right_bindings.end());
	return left_bindings;
}

void LogicalUnconditionalJoin::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	types.insert(types.end(), children[1]->types.begin(), children[1]->types.end());
}

} // namespace duckdb
