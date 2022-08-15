#include "duckdb/planner/operator/logical_cross_product.hpp"

namespace duckdb {

LogicalCrossProduct::LogicalCrossProduct(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
	D_ASSERT(left);
	D_ASSERT(right);
	children.push_back(move(left));
	children.push_back(move(right));
}

vector<ColumnBinding> LogicalCrossProduct::GetColumnBindings() {
	auto left_bindings = children[0]->GetColumnBindings();
	auto right_bindings = children[1]->GetColumnBindings();
	left_bindings.insert(left_bindings.end(), right_bindings.begin(), right_bindings.end());
	return left_bindings;
}

void LogicalCrossProduct::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	types.insert(types.end(), children[1]->types.begin(), children[1]->types.end());
}

unique_ptr<LogicalOperator> LogicalCrossProduct::Create(unique_ptr<LogicalOperator> left,
                                                        unique_ptr<LogicalOperator> right) {
	if (left->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		return right;
	}
	if (right->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		return left;
	}
	return make_unique<LogicalCrossProduct>(move(left), move(right));
}

} // namespace duckdb
