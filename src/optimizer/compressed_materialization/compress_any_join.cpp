#include "duckdb/optimizer/compressed_materialization.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"

namespace duckdb {

void CompressedMaterialization::CompressAnyJoin(unique_ptr<LogicalOperator> &op) {
	auto &join = op->Cast<LogicalAnyJoin>();

	column_binding_set_t referenced_bindings;
	GetReferencedBindings(*join.condition, referenced_bindings);

	// Create info for compression
	CompressedMaterializationInfo info(*op, {1}, referenced_bindings);

	// Create binding mapping
	const auto bindings = join.GetColumnBindings();
	const auto &types = join.types;
	D_ASSERT(bindings.size() == types.size());
	for (idx_t col_idx = 0; col_idx < bindings.size(); col_idx++) {
		// Any join does not change bindings, input binding is output binding
		info.binding_map.emplace(bindings[col_idx], CMBindingInfo(bindings[col_idx], types[col_idx]));
	}

	// Now try to compress
	CreateProjections(op, info);
}

} // namespace duckdb
