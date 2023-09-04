#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

LogicalOrder::LogicalOrder(vector<BoundOrderByNode> orders)
    : LogicalOperator(LogicalOperatorType::LOGICAL_ORDER_BY), orders(std::move(orders)) {
}

vector<ColumnBinding> LogicalOrder::GetColumnBindings() {
	auto child_bindings = children[0]->GetColumnBindings();
	if (projections.empty()) {
		return child_bindings;
	}

	vector<ColumnBinding> result;
	for (auto &col_idx : projections) {
		result.push_back(child_bindings[col_idx]);
	}
	return result;
}

string LogicalOrder::ParamsToString() const {
	string result = "ORDERS:\n";
	for (idx_t i = 0; i < orders.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += orders[i].expression->GetName();
	}
	return result;
}

void LogicalOrder::ResolveTypes() {
	const auto child_types = children[0]->types;
	if (projections.empty()) {
		types = child_types;
	} else {
		for (auto &col_idx : projections) {
			types.push_back(child_types[col_idx]);
		}
	}
}

} // namespace duckdb
