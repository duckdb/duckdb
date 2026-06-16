#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

static bool FilterNullRejectsExpression(const Expression &filter, const Expression &expr) {
	if (filter.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &conjunction = filter.Cast<BoundConjunctionExpression>();
		for (auto &child : conjunction.GetChildren()) {
			if (FilterNullRejectsExpression(*child, expr)) {
				return true;
			}
		}
		return false;
	}
	if (filter.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		auto &conjunction = filter.Cast<BoundConjunctionExpression>();
		if (conjunction.GetChildren().empty()) {
			return false;
		}
		for (auto &child : conjunction.GetChildren()) {
			if (!FilterNullRejectsExpression(*child, expr)) {
				return false;
			}
		}
		return true;
	}
	if (filter.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) {
		auto &op = filter.Cast<BoundOperatorExpression>();
		return !op.GetChildren().empty() && Expression::Equals(*op.GetChildren()[0], expr);
	}
	if (!BoundComparisonExpression::IsComparison(filter)) {
		return false;
	}
	if (filter.GetExpressionType() == ExpressionType::COMPARE_DISTINCT_FROM ||
	    filter.GetExpressionType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		return false;
	}
	auto &comparison = filter.Cast<BoundFunctionExpression>();
	return Expression::Equals(BoundComparisonExpression::Left(comparison), expr) ||
	       Expression::Equals(BoundComparisonExpression::Right(comparison), expr);
}

static bool GetColumnRefBinding(const Expression &expr, ColumnBinding &binding) {
	if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	auto &colref = expr.Cast<BoundColumnRefExpression>();
	if (colref.Depth() != 0) {
		return false;
	}
	binding = colref.Binding();
	return true;
}

static bool ExpressionIsNotNull(ClientContext &context, LogicalOperator &op, const Expression &expr) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		if (projection.children.size() != 1) {
			return false;
		}
		ColumnBinding binding;
		if (!GetColumnRefBinding(expr, binding)) {
			return false;
		}
		auto projection_bindings = projection.GetColumnBindings();
		for (idx_t idx = 0; idx < projection_bindings.size(); idx++) {
			if (projection_bindings[idx] == binding) {
				return ExpressionIsNotNull(context, *projection.children[0], *projection.expressions[idx]);
			}
		}
		return false;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto &filter = op.Cast<LogicalFilter>();
		for (auto &filter_expr : filter.expressions) {
			if (FilterNullRejectsExpression(*filter_expr, expr)) {
				return true;
			}
		}
		return filter.children.size() == 1 && ExpressionIsNotNull(context, *filter.children[0], expr);
	}
	case LogicalOperatorType::LOGICAL_GET: {
		ColumnBinding binding;
		if (!GetColumnRefBinding(expr, binding)) {
			return false;
		}
		auto &get = op.Cast<LogicalGet>();
		if (binding.table_index != get.table_index) {
			return false;
		}
		if (get.table_filters.HasFilter(binding.column_index)) {
			auto column_expr = make_uniq<BoundColumnRefExpression>(expr.GetReturnType(), binding);
			auto filter_expr =
			    get.table_filters.GetFilterByColumnIndex(binding.column_index).ToExpression(*column_expr);
			if (FilterNullRejectsExpression(*filter_expr, expr)) {
				return true;
			}
		}
		auto table = get.GetTable();
		if (!table) {
			return false;
		}
		auto &column_index = get.GetColumnIndex(binding);
		if (!column_index.HasPrimaryIndex() || column_index.HasChildren() ||
		    column_index.GetPrimaryIndex() == DConstants::INVALID_INDEX) {
			return false;
		}
		auto stats = table->GetStatistics(context, column_index.GetPrimaryIndex());
		return stats && !stats->CanHaveNull();
	}
	default:
		return false;
	}
}

static void SimplifyNullSafeSemiJoinConditions(ClientContext &context, LogicalComparisonJoin &join) {
	D_ASSERT(join.join_type == JoinType::SEMI);
	for (auto &cond : join.conditions) {
		if (!cond.IsComparison() || cond.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			continue;
		}
		// Once a MARK join is reduced to SEMI, a null-safe equality is equivalent to regular equality if either
		// join key is known not to be NULL. Regular equality unlocks the existing runtime-filter infrastructure.
		if (!ExpressionIsNotNull(context, *join.children[0], cond.GetLHS()) &&
		    !ExpressionIsNotNull(context, *join.children[1], cond.GetRHS())) {
			continue;
		}
		cond =
		    JoinCondition(cond.LeftReference()->Copy(), cond.RightReference()->Copy(), ExpressionType::COMPARE_EQUAL);
	}
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownMarkJoin(unique_ptr<LogicalOperator> op,
                                                             unordered_set<TableIndex> &left_bindings,
                                                             unordered_set<TableIndex> &right_bindings) {
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
				if (op_expr.GetChildren()[0]->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
					// the filter is NOT(marker), check the join conditions
					bool all_null_values_are_equal = true;
					for (auto &cond : comp_join.conditions) {
						if (!cond.IsComparison()) {
							continue;
						}
						if (cond.GetComparisonType() != ExpressionType::COMPARE_DISTINCT_FROM &&
						    cond.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
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
	if (join.join_type == JoinType::SEMI) {
		SimplifyNullSafeSemiJoinConditions(GetContext(), comp_join);
	}
	return PushFinalFilters(std::move(op));
}

} // namespace duckdb
