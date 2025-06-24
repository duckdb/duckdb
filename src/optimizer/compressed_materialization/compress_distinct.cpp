#include "duckdb/optimizer/compressed_materialization.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"

namespace duckdb {

void CompressedMaterialization::CompressDistinct(unique_ptr<LogicalOperator> &op) {
	auto &distinct = op->Cast<LogicalDistinct>();
	auto &distinct_targets = distinct.distinct_targets;

	column_binding_set_t referenced_bindings;
	for (auto &target : distinct_targets) {
		if (target->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) { // LCOV_EXCL_START
			GetReferencedBindings(*target, referenced_bindings);
		} // LCOV_EXCL_STOP
	}

	if (distinct.order_by) {
		for (auto &order : distinct.order_by->orders) {
			if (order.expression->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) { // LCOV_EXCL_START
				GetReferencedBindings(*order.expression, referenced_bindings);
			} // LCOV_EXCL_STOP
		}
	}

	// Create info for compression
	CompressedMaterializationInfo info(*op, {0}, referenced_bindings);

	// Create binding mapping
	const auto bindings = distinct.GetColumnBindings();
	const auto &types = distinct.types;
	D_ASSERT(bindings.size() == types.size());
	for (idx_t col_idx = 0; col_idx < bindings.size(); col_idx++) {
		// Distinct does not change bindings, input binding is output binding
		info.binding_map.emplace(bindings[col_idx], CMBindingInfo(bindings[col_idx], types[col_idx]));
	}

	// Now try to compress
	CreateProjections(op, info);
}

} // namespace duckdb
