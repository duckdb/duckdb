#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownMarkJoin(unique_ptr<LogicalOperator> op,
                                                             unordered_set<idx_t> &left_bindings,
                                                             unordered_set<idx_t> &right_bindings) {
	auto op_bindings = op->GetColumnBindings();
	auto &join = op->Cast<LogicalJoin>();
	auto &comp_join = op->Cast<LogicalComparisonJoin>();
	D_ASSERT(join.join_type == JoinType::MARK);
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN || op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN);

	right_bindings.insert(comp_join.mark_index);
	FilterPushdown left_pushdown(optimizer, convert_mark_joins), right_pushdown(optimizer, convert_mark_joins);
#ifdef DEBUG
	bool simplified_mark_join = false;
#endif
	// now check the set of filters
	for (idx_t i = 0; i < filters.size(); i++) {
		auto side = JoinSide::GetJoinSide(filters[i]->bindings, left_bindings, right_bindings);
		if (side == JoinSide::LEFT) {
			// bindings match left side: push into left
			left_pushdown.filters.push_back(std::move(filters[i]));
			// erase the filter from the list of filters
			filters.erase_at(i);
			i--;
		} else if (side == JoinSide::RIGHT) {
#ifdef DEBUG
			D_ASSERT(!simplified_mark_join);
#endif
			// this filter references the marker
			// we can turn this into a SEMI join if the filter is on only the marker
			if (filters[i]->filter->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF && convert_mark_joins &&
			    comp_join.convert_mark_to_semi) {
				// filter just references the marker: turn into semi join
#ifdef DEBUG
				simplified_mark_join = true;
#endif
				join.join_type = JoinType::SEMI;
				filters.erase_at(i);
				i--;
				continue;
			}
			// if the filter is on NOT(marker) AND the join conditions are all set to "null_values_are_equal" we can
			// turn this into an ANTI join if all join conditions have null_values_are_equal=true, then the result of
			// the MARK join is always TRUE or FALSE, and never NULL this happens in the case of a correlated EXISTS
			// clause
			if (filters[i]->filter->GetExpressionType() == ExpressionType::OPERATOR_NOT) {
				auto &op_expr = filters[i]->filter->Cast<BoundOperatorExpression>();
				if (op_expr.children[0]->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
					// the filter is NOT(marker), check the join conditions
					bool all_null_values_are_equal = true;
					for (auto &cond : comp_join.conditions) {
						if (cond.comparison != ExpressionType::COMPARE_DISTINCT_FROM &&
						    cond.comparison != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
							all_null_values_are_equal = false;
							break;
						}
					}
					if (all_null_values_are_equal && convert_mark_joins && comp_join.convert_mark_to_semi) {
#ifdef DEBUG
						simplified_mark_join = true;
#endif
						// all null values are equal, convert to ANTI join
						join.join_type = JoinType::ANTI;
						filters.erase_at(i);
						i--;
						continue;
					}
				}
			}
		}
	}
	op->children[0] = left_pushdown.Rewrite(std::move(op->children[0]));
	op->children[1] = right_pushdown.Rewrite(std::move(op->children[1]));
	return PushFinalFilters(std::move(op));
}

} // namespace duckdb
