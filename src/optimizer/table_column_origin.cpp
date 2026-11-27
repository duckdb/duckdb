#include "duckdb/optimizer/table_column_origin.hpp"

#include "duckdb/optimizer/aggregate_rewrite_helper.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include <algorithm>

namespace duckdb {

optional_idx GetDirectReferenceIndex(const Expression &expression, LogicalOperator &input) {
	return AggregateRewriteHelper::GetDirectReferenceIndex(expression, input);
}

optional_idx GetInvertibleReferenceIndex(const Expression &expression, LogicalOperator &input) {
	auto current = &expression;
	while (true) {
		auto result = GetDirectReferenceIndex(*current, input);
		if (result.IsValid() || !BoundCastExpression::IsCast(*current)) {
			return result;
		}
		auto &cast = current->Cast<BoundFunctionExpression>();
		if (!BoundCastExpression::CastIsInvertible(BoundCastExpression::SourceType(cast),
		                                           BoundCastExpression::TargetType(cast))) {
			return optional_idx();
		}
		current = &BoundCastExpression::Child(cast);
	}
}

optional<TableColumnOrigin> GetTableColumnOrigin(LogicalOperator &root, idx_t output_idx, bool allow_filtered) {
	auto op = &root;
	while (true) {
		auto bindings = op->GetColumnBindings();
		if (output_idx >= bindings.size()) {
			return nullopt;
		}
		if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &projection = op->Cast<LogicalProjection>();
			if (projection.children.size() != 1 || output_idx >= projection.expressions.size()) {
				return nullopt;
			}
			auto child_idx =
			    allow_filtered
			        ? GetInvertibleReferenceIndex(*projection.expressions[output_idx], *projection.children[0])
			        : GetDirectReferenceIndex(*projection.expressions[output_idx], *projection.children[0]);
			if (!child_idx.IsValid()) {
				return nullopt;
			}
			output_idx = child_idx.GetIndex();
			op = projection.children[0].get();
			continue;
		}
		if (op->type == LogicalOperatorType::LOGICAL_FILTER && allow_filtered) {
			auto &filter = op->Cast<LogicalFilter>();
			if (filter.children.size() != 1) {
				return nullopt;
			}
			if (!filter.projection_map.empty()) {
				if (output_idx >= filter.projection_map.size()) {
					return nullopt;
				}
				output_idx = filter.projection_map[output_idx].GetIndex();
			}
			op = filter.children[0].get();
			continue;
		}
		if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN && allow_filtered) {
			auto binding = bindings[output_idx];
			optional_ptr<LogicalOperator> child_op;
			for (auto &child : op->children) {
				auto child_bindings = child->GetColumnBindings();
				auto entry = std::find(child_bindings.begin(), child_bindings.end(), binding);
				if (entry != child_bindings.end()) {
					child_op = child.get();
					output_idx = NumericCast<idx_t>(entry - child_bindings.begin());
					break;
				}
			}
			if (!child_op) {
				return nullopt;
			}
			op = child_op.get();
			continue;
		}
		if (op->type != LogicalOperatorType::LOGICAL_GET) {
			return nullopt;
		}
		auto &get = op->Cast<LogicalGet>();
		auto table = get.GetTable();
		if (!table || !get.children.empty() || get.GetColumnIds().empty() || !get.scan_partition_indices.empty()) {
			return nullopt;
		}
		if (!allow_filtered &&
		    (get.table_filters.HasFilters() || (get.dynamic_filters && get.dynamic_filters->HasFilters()) ||
		     get.extra_info.sample_options || !get.extra_info.file_filters.empty() ||
		     get.extra_info.file_filter_expressions || get.has_pushed_projection || get.at_clause)) {
			return nullopt;
		}
		// Output positions can be remapped by projection_ids; the binding identifies the scanned column.
		return TableColumnOrigin {*table, get.GetColumnIndex(bindings[output_idx])};
	}
}

optional<TableColumnOrigin> GetTableColumnOrigin(LogicalOperator &op, const Expression &expression,
                                                 bool allow_filtered) {
	auto output_idx = GetDirectReferenceIndex(expression, op);
	return output_idx.IsValid() ? GetTableColumnOrigin(op, output_idx.GetIndex(), allow_filtered) : nullopt;
}

bool SameOrigin(const TableColumnOrigin &left, const TableColumnOrigin &right) {
	return &left.table.get() == &right.table.get() && left.column == right.column;
}

} // namespace duckdb
