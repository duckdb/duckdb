#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

LogicalOrder::LogicalOrder(vector<BoundOrderByNode> orders)
    : LogicalOperator(LogicalOperatorType::LOGICAL_ORDER_BY), orders(std::move(orders)) {
}

vector<ColumnBinding> LogicalOrder::GetColumnBindings() {
	auto child_bindings = children[0]->GetColumnBindings();
	if (!HasProjectionMap()) {
		return child_bindings;
	}
	return MapBindings(child_bindings, projection_map);
}

InsertionOrderPreservingMap<string> LogicalOrder::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string orders_info;
	for (idx_t i = 0; i < orders.size(); i++) {
		if (i > 0) {
			orders_info += "\n";
		}
		orders_info += orders[i].expression->GetName();
	}
	result["__order_by__"] = orders_info;
	SetParamsEstimatedCardinality(result);
	return result;
}

void LogicalOrder::ResolveTypes() {
	const auto child_types = children[0]->types;
	if (!HasProjectionMap()) {
		types = child_types;
	} else {
		types = MapTypes(child_types, projection_map);
	}
}

} // namespace duckdb
