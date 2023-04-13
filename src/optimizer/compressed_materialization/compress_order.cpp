#include "duckdb/optimizer/compressed_materialization.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

static void GetReferencedBindings(const Expression &expression, vector<ColumnBinding> &referenced_bindings) {
	ExpressionIterator::EnumerateChildren(expression, [&](const Expression &child) {
		if (child.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			const auto &col_ref = (BoundColumnRefExpression &)child;
			referenced_bindings.emplace_back(col_ref.binding);
		}
	});
}

void CompressedMaterialization::CompressOrder(unique_ptr<LogicalOperator> &op) {
	auto &order = (LogicalOrder &)*op;

	// Find all bindings referenced by non-colref expressions in the order nodes
	// These are excluded from compression by projection
	// But we can try to compress the expression directly
	vector<ColumnBinding> referenced_bindings;
	for (idx_t order_node_idx = 0; order_node_idx < order.orders.size(); order_node_idx++) {
		auto &bound_order = order.orders[order_node_idx];
		auto &order_expression = *bound_order.expression;
		if (order_expression.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			continue; // Will be compressed generically
		}

		// Mark the bindings referenced by the non-colref expression so they won't be modified
		GetReferencedBindings(order_expression, referenced_bindings);

		// The non-colref expression won't be compressed generically, so try to compress it here
		if (!bound_order.stats) {
			continue; // Can't compress without stats
		}

		// Try to compress, if successful, replace the expression
		auto compress_expr = GetCompressExpression(order_expression.Copy(), *bound_order.stats);
		if (compress_expr) {
			bound_order.expression = std::move(compress_expr);
		}
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
}

} // namespace duckdb
