#include "duckdb/optimizer/compressed_materialization.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"

namespace duckdb {

void CompressedMaterialization::CompressDistinct(unique_ptr<LogicalOperator> &op) {
	auto &distinct = op->Cast<LogicalDistinct>();
	auto &distinct_targets = distinct.distinct_targets;

	// A VARIANT that is compressed to its shredded type compares like the variant_comparator of it,
	// so its wrapper can be stripped and the column deduplicated directly
	vector<bool> strip_variant_wrapper(distinct_targets.size(), false);
	for (idx_t target_idx = 0; target_idx < distinct_targets.size(); target_idx++) {
		auto stats = GetVariantWrapperStats(*distinct_targets[target_idx]);
		strip_variant_wrapper[target_idx] = stats && CanCompressVariant(*stats);
	}

	column_binding_set_t referenced_bindings;
	for (idx_t target_idx = 0; target_idx < distinct_targets.size(); target_idx++) {
		auto &target = distinct_targets[target_idx];
		if (target->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF && !strip_variant_wrapper[target_idx]) {
			GetReferencedBindings(*target, referenced_bindings);
		}
	}

	if (distinct.order_by) {
		for (auto &order : distinct.order_by->orders) {
			if (order.expression->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) { // LCOV_EXCL_START
				GetReferencedBindings(*order.expression, referenced_bindings);
			} // LCOV_EXCL_STOP
		}
	}

	for (idx_t target_idx = 0; target_idx < distinct_targets.size(); target_idx++) {
		if (!strip_variant_wrapper[target_idx]) {
			continue;
		}
		auto colref = TryGetVariantWrapperColumnRef(*distinct_targets[target_idx]);
		if (referenced_bindings.find(colref->Binding()) != referenced_bindings.end()) {
			// The column is used elsewhere, so it is not compressed
			continue;
		}
		nested_variant_bindings.insert(colref->Binding());
		distinct_targets[target_idx] = colref->Copy();
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
	nested_variant_bindings.clear();
}

} // namespace duckdb
