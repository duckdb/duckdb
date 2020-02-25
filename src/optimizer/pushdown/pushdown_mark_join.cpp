#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

using namespace duckdb;
using namespace std;

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownMarkJoin(unique_ptr<LogicalOperator> op,
                                                             unordered_set<idx_t> &left_bindings,
                                                             unordered_set<idx_t> &right_bindings) {
	auto &join = (LogicalJoin &)*op;
	auto &comp_join = (LogicalComparisonJoin &)*op;
	assert(join.join_type == JoinType::MARK);
	assert(op->type == LogicalOperatorType::COMPARISON_JOIN || op->type == LogicalOperatorType::DELIM_JOIN);

	right_bindings.insert(comp_join.mark_index);
	FilterPushdown left_pushdown(optimizer), right_pushdown(optimizer);
	bool found_mark_reference = false;
	// now check the set of filters
	for (idx_t i = 0; i < filters.size(); i++) {
		auto side = JoinSide::GetJoinSide(filters[i]->bindings, left_bindings, right_bindings);
		if (side == JoinSide::LEFT) {
			// bindings match left side: push into left
			left_pushdown.filters.push_back(move(filters[i]));
			// erase the filter from the list of filters
			filters.erase(filters.begin() + i);
			i--;
		} else if (side == JoinSide::RIGHT) {
			// there can only be at most one filter referencing the marker
			assert(!found_mark_reference);
			found_mark_reference = true;
			// this filter references the marker
			// we can turn this into a SEMI join if the filter is on only the marker
			if (filters[i]->filter->type == ExpressionType::BOUND_COLUMN_REF) {
				// filter just references the marker: turn into semi join
				join.join_type = JoinType::SEMI;
				filters.erase(filters.begin() + i);
				i--;
				continue;
			}
			// if the filter is on NOT(marker) AND the join conditions are all set to "null_values_are_equal" we can
			// turn this into an ANTI join if all join conditions have null_values_are_equal=true, then the result of
			// the MARK join is always TRUE or FALSE, and never NULL this happens in the case of a correlated EXISTS
			// clause
			if (filters[i]->filter->type == ExpressionType::OPERATOR_NOT) {
				auto &op_expr = (BoundOperatorExpression &)*filters[i]->filter;
				if (op_expr.children[0]->type == ExpressionType::BOUND_COLUMN_REF) {
					// the filter is NOT(marker), check the join conditions
					bool all_null_values_are_equal = true;
					for (auto &cond : comp_join.conditions) {
						if (!cond.null_values_are_equal) {
							all_null_values_are_equal = false;
							break;
						}
					}
					if (all_null_values_are_equal) {
						// all null values are equal, convert to ANTI join
						join.join_type = JoinType::ANTI;
						filters.erase(filters.begin() + i);
						i--;
						continue;
					}
				}
			}
		}
	}
	op->children[0] = left_pushdown.Rewrite(move(op->children[0]));
	op->children[1] = right_pushdown.Rewrite(move(op->children[1]));
	return FinishPushdown(move(op));
}
