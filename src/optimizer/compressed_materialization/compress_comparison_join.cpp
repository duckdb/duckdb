#include "duckdb/optimizer/compressed_materialization.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

static void PopulateBindingMap(CompressedMaterializationInfo &info, const vector<ColumnBinding> &bindings_out,
                               const vector<LogicalType> &types, LogicalOperator &op_in) {
	const auto rhs_bindings_in = op_in.GetColumnBindings();
	for (const auto &rhs_binding : rhs_bindings_in) {
		// Joins do not change bindings, input binding is output binding
		for (idx_t col_idx_out = 0; col_idx_out < bindings_out.size(); col_idx_out++) {
			const auto &binding_out = bindings_out[col_idx_out];
			if (binding_out == rhs_binding) {
				// This one is projected out, add it to map
				info.binding_map.emplace(rhs_binding, CMBindingInfo(binding_out, types[col_idx_out]));
			}
		}
	}
}

void CompressedMaterialization::CompressComparisonJoin(unique_ptr<LogicalOperator> &op) {
	auto &join = op->Cast<LogicalComparisonJoin>();

	// Find all bindings referenced by non-colref expressions in the conditions
	// These are excluded from compression by projection
	// But we can try to compress the expression directly
	vector<ColumnBinding> referenced_bindings;
	for (const auto &condition : join.conditions) {
		if (condition.left->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			auto &colref = condition.left->Cast<BoundColumnRefExpression>();
			referenced_bindings.emplace_back(colref.binding);
		} else {
			GetReferencedBindings(*condition.left, referenced_bindings);
		}
		if (condition.right->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			auto &colref = condition.right->Cast<BoundColumnRefExpression>();
			referenced_bindings.emplace_back(colref.binding);
		} else {
			GetReferencedBindings(*condition.right, referenced_bindings);
		}

		// TODO compress conditions too
		// TODO: update join stats if compress
	}

	// Create info for compression
	CompressedMaterializationInfo info(*op, {0, 1}, referenced_bindings);

	const auto bindings_out = join.GetColumnBindings();
	const auto &types = join.types;
	PopulateBindingMap(info, bindings_out, types, *op->children[0]);
	PopulateBindingMap(info, bindings_out, types, *op->children[1]);

	// Now try to compress
	CreateProjections(op, info);
}

} // namespace duckdb
