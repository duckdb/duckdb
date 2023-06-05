#include "duckdb/optimizer/compressed_materialization.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

void CompressedMaterialization::CompressOrder(unique_ptr<LogicalOperator> &op) {
	auto &order = op->Cast<LogicalOrder>();

	// Find all bindings referenced by non-colref expressions in the order nodes
	// These are excluded from compression by projection
	// But we can try to compress the expression directly
	column_binding_set_t referenced_bindings;
	for (idx_t order_node_idx = 0; order_node_idx < order.orders.size(); order_node_idx++) {
		auto &bound_order = order.orders[order_node_idx];
		auto &order_expression = *bound_order.expression;
		if (order_expression.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			continue; // Will be compressed generically
		}

		// Mark the bindings referenced by the non-colref expression so they won't be modified
		GetReferencedBindings(order_expression, referenced_bindings);
	}

	// Create info for compression
	CompressedMaterializationInfo info(*op, {0}, referenced_bindings);

	// Create binding mapping
	const auto bindings = order.GetColumnBindings();
	const auto &types = order.types;
	D_ASSERT(bindings.size() == types.size());
	for (idx_t col_idx = 0; col_idx < bindings.size(); col_idx++) {
		// Order does not change bindings, input binding is output binding
		info.binding_map.emplace(bindings[col_idx], CMBindingInfo(bindings[col_idx], types[col_idx]));
	}

	// Now try to compress
	CreateProjections(op, info);

	// Update order statistics
	UpdateOrderStats(op);
}

void CompressedMaterialization::UpdateOrderStats(unique_ptr<LogicalOperator> &op) {
	if (op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}

	// Update order stats if compressed
	auto &compressed_order = op->children[0]->Cast<LogicalOrder>();
	for (idx_t order_node_idx = 0; order_node_idx < compressed_order.orders.size(); order_node_idx++) {
		auto &bound_order = compressed_order.orders[order_node_idx];
		auto &order_expression = *bound_order.expression;
		if (order_expression.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		auto &colref = order_expression.Cast<BoundColumnRefExpression>();
		auto it = statistics_map.find(colref.binding);
		if (it != statistics_map.end() && it->second) {
			bound_order.stats = it->second->ToUnique();
		}
	}
}

} // namespace duckdb
