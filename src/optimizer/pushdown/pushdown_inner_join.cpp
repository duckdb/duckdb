#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/subquery/column_binding_layout.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownInnerJoin(unique_ptr<LogicalOperator> op,
                                                              unordered_set<TableIndex> &left_bindings,
                                                              unordered_set<TableIndex> &right_bindings) {
	auto &join = op->Cast<LogicalJoin>();
	D_ASSERT(join.join_type == JoinType::INNER);
	if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		op = PushFiltersIntoDelimJoin(std::move(op));
		return FinishPushdown(std::move(op));
	}
	// inner join: gather all the conditions of the inner join and add to the filter list
	if (op->type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
		auto &any_join = join.Cast<LogicalAnyJoin>();
		// any join: only one filter to add
		if (AddFilter(std::move(any_join.condition)) == FilterResult::UNSATISFIABLE) {
			// filter statically evaluates to false, strip tree
			return make_uniq<LogicalEmptyResult>(std::move(op));
		}
	} else {
		// comparison join
		D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
		auto &comp_join = join.Cast<LogicalComparisonJoin>();
		// turn the conditions into filters
		for (auto &i : comp_join.conditions) {
			auto condition = JoinCondition::CreateExpression(std::move(i));
			if (AddFilter(std::move(condition)) == FilterResult::UNSATISFIABLE) {
				// filter statically evaluates to false, strip tree
				return make_uniq<LogicalEmptyResult>(std::move(op));
			}
		}
	}
	GenerateFilters();

	auto left_bindings_before = op->children[0]->GetColumnBindings();
	auto right_bindings_before = op->children[1]->GetColumnBindings();
	// turn the inner join into a cross product
	auto cross_product = make_uniq<LogicalCrossProduct>(std::move(op->children[0]), std::move(op->children[1]));

	// preserve the estimated cardinality of the operator
	if (op->has_estimated_cardinality) {
		cross_product->SetEstimatedCardinality(op->estimated_cardinality);
	}

	// then push down cross product
	auto result = PushdownCrossProduct(std::move(cross_product));

	// Carry the projection maps over when the join is re-created.
	switch (result->type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &result_join = result->Cast<LogicalJoin>();
		result_join.left_projection_map = std::move(join.left_projection_map);
		result_join.right_projection_map = std::move(join.right_projection_map);
		LogicalOperatorVisitor::RemapProjectionMap(result_join.left_projection_map, left_bindings_before,
		                                           result_join.children[0]->GetColumnBindings());
		LogicalOperatorVisitor::RemapProjectionMap(result_join.right_projection_map, right_bindings_before,
		                                           result_join.children[1]->GetColumnBindings());
		return result;
	}
	default:
		break;
	}
	if (!join.HasProjectionMap()) {
		return result;
	}
	// Other replacements must expose the same columns in the same order as the original join.
	auto projected_bindings = LogicalOperator::MapBindings(left_bindings_before, join.left_projection_map);
	auto projected_right = LogicalOperator::MapBindings(right_bindings_before, join.right_projection_map);
	projected_bindings.insert(projected_bindings.end(), projected_right.begin(), projected_right.end());
	auto result_bindings = result->GetColumnBindings();
	if (result_bindings == projected_bindings) {
		return result;
	}
	auto projection_map = ColumnBindingLayout(std::move(result_bindings)).CreateProjectionMap(projected_bindings);
	if (result->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
		auto &empty = result->Cast<LogicalEmptyResult>();
		return make_uniq<LogicalEmptyResult>(LogicalOperator::MapTypes(empty.return_types, projection_map),
		                                     std::move(projected_bindings));
	}
	return LogicalJoin::PromoteJoinSide(std::move(result), std::move(projection_map));
}

} // namespace duckdb
