#include "duckdb/planner/operator/logical_cross_product.hpp"

namespace duckdb {

LogicalCrossProduct::LogicalCrossProduct() : LogicalOperator(LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
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

} // namespace duckdb
