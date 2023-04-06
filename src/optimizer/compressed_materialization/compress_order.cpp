#include "duckdb/optimizer/compressed_materialization_optimizer.hpp"
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

void CompressedMaterializationOptimizer::CompressOrder(unique_ptr<LogicalOperator> *op_ptr) {
	auto &order = (LogicalOrder &)**op_ptr;

	// Find all bindings referenced by non-colref expressions in the order nodes
	// These are excluded from compression by projection
	// But we can try to compress the expression directly
	vector<ColumnBinding> referenced_bindings;
	for (idx_t order_node_idx = 0; order_node_idx < order.orders.size(); order_node_idx++) {
		const auto &order_expression = *order.orders[order_node_idx].expression;
		if (order_expression.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			GetReferencedBindings(order_expression, referenced_bindings);
			// TODO try to compress this order_expression right now
		}
	}

	// Try to compress
	CompressedMaterializationInfo info(**op_ptr, {0}, false, referenced_bindings);
	CreateProjections(op_ptr, info);
}

} // namespace duckdb
