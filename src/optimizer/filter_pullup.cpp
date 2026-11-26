#include "duckdb/optimizer/filter_pullup.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/subquery/column_binding_layout.hpp"

namespace duckdb {

static bool HasComputedJoinOperands(const Expression &lhs, const Expression &rhs) {
	return !lhs.IsScalar() && !rhs.IsScalar() &&
	       (lhs.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF ||
	        rhs.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF);
}

static bool NeedsEarlyJoinKeys(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (join.join_type != JoinType::INNER) {
			return false;
		}
		for (auto &condition : join.conditions) {
			if (condition.IsComparison() && condition.GetComparisonType() == ExpressionType::COMPARE_EQUAL &&
			    HasComputedJoinOperands(condition.GetLHS(), condition.GetRHS())) {
				return true;
			}
		}
		return false;
	}
	if (op.type != LogicalOperatorType::LOGICAL_FILTER ||
	    op.children[0]->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		return false;
	}
	auto &cross = *op.children[0];
	unordered_set<TableIndex> left_bindings, right_bindings;
	for (auto &expression : op.expressions) {
		if (expression->GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
			continue;
		}
		auto &comparison = expression->Cast<BoundFunctionExpression>();
		auto &lhs = BoundComparisonExpression::Left(comparison);
		auto &rhs = BoundComparisonExpression::Right(comparison);
		if (!HasComputedJoinOperands(lhs, rhs)) {
			continue;
		}
		if (left_bindings.empty() && right_bindings.empty()) {
			LogicalJoin::GetTableReferences(*cross.children[0], left_bindings);
			LogicalJoin::GetTableReferences(*cross.children[1], right_bindings);
		}
		auto lhs_side = JoinSide::GetCurrentJoinSide(lhs, left_bindings, right_bindings);
		auto rhs_side = JoinSide::GetCurrentJoinSide(rhs, left_bindings, right_bindings);
		if ((lhs_side == JoinSide::LEFT && rhs_side == JoinSide::RIGHT) ||
		    (lhs_side == JoinSide::RIGHT && rhs_side == JoinSide::LEFT)) {
			return true;
		}
	}
	return false;
}

unique_ptr<LogicalOperator> FilterPullup::Rewrite(unique_ptr<LogicalOperator> op) {
	if (op->HasProjectionMap()) {
		return FinishPullup(std::move(op));
	}
	if (!can_pullup && !can_add_column && NeedsEarlyJoinKeys(*op)) {
		// Carry hidden keys within this join region and keep its output bindings unchanged.
		auto output = op->GetColumnBindings();
		FilterPullup join_pullup(context, false, true);
		auto rewritten = join_pullup.Rewrite(std::move(op));
		ColumnBindingLayout layout(rewritten->GetColumnBindings());
		if (layout.HasSameLayout(output)) {
			return rewritten;
		}
		return LogicalJoin::PromoteJoinSide(std::move(rewritten), layout.CreateProjectionMap(output));
	}
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_FILTER:
		return PullupFilter(std::move(op));
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return PullupProjection(std::move(op));
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return PullupCrossProduct(std::move(op));
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		return PullupJoin(std::move(op));
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT:
		return PullupSetOperation(std::move(op));
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return PullupAggregate(std::move(op));
	case LogicalOperatorType::LOGICAL_UNION:
		return PullupUnion(std::move(op));
	case LogicalOperatorType::LOGICAL_WINDOW:
		return PullupWindow(std::move(op));
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		// we can just pull directly through these operations without any rewriting
		op->children[0] = Rewrite(std::move(op->children[0]));
		return op;
	}
	default:
		return FinishPullup(std::move(op));
	}
}

unique_ptr<LogicalOperator> FilterPullup::PullupJoin(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN || op->type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
	auto &join = op->Cast<LogicalJoin>();
	if (op->type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
		if (op->Cast<LogicalAnyJoin>().condition->IsVolatile()) {
			return FinishPullup(std::move(op));
		}
	} else {
		for (auto &condition : op->Cast<LogicalComparisonJoin>().conditions) {
			if (condition.IsComparison() ? condition.GetLHS().IsVolatile() || condition.GetRHS().IsVolatile()
			                             : condition.GetJoinExpression().IsVolatile()) {
				return FinishPullup(std::move(op));
			}
		}
	}
	if (can_add_column && (join.join_type != JoinType::INNER || op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN)) {
		return FinishPullup(std::move(op));
	}

	switch (join.join_type) {
	case JoinType::INNER:
		if (op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
			//	We can only move filters through the left side of AsOf joins.
			return PullupFromLeft(std::move(op));
		} else {
			return PullupInnerJoin(std::move(op));
		}
	case JoinType::SEMI:
		if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			return PullupSemiJoin(std::move(op));
		}
		return PullupFromLeft(std::move(op));
	case JoinType::LEFT:
	case JoinType::ANTI:
		return PullupFromLeft(std::move(op));
	default:
		// unsupported join type: call children pull up
		return FinishPullup(std::move(op));
	}
}

unique_ptr<LogicalOperator> FilterPullup::PullupSemiJoin(unique_ptr<LogicalOperator> op) {
	auto &join = op->Cast<LogicalComparisonJoin>();
	D_ASSERT(join.join_type == JoinType::SEMI);
	FilterPullup left_pullup(context, true, can_add_column);
	FilterPullup right_pullup(context, true, can_add_column);
	op->children[0] = left_pullup.Rewrite(std::move(op->children[0]));
	op->children[1] = right_pullup.Rewrite(std::move(op->children[1]));

	auto &left_filters = left_pullup.filters_expr_pullup;
	if (!right_pullup.filters_expr_pullup.empty()) {
		// a left row with a match passes what the filters on the right imply for the left side
		vector<reference<const Expression>> right_filters;
		for (auto &filter : right_pullup.filters_expr_pullup) {
			right_filters.push_back(*filter);
		}
		FilterCombiner::GenerateJoinFilters(
		    join.conditions, right_filters, JoinSide::LEFT,
		    [&](unique_ptr<Expression> left_filter) { left_filters.push_back(std::move(left_filter)); });
		op->children[1] = GeneratePullupFilter(std::move(op->children[1]), right_pullup.filters_expr_pullup);
	}
	vector<unique_ptr<Expression>> constraints;
	for (auto &filter : left_filters) {
		if (FilterCombiner::CanCopyFilter(*filter)) {
			constraints.push_back(filter->Copy());
		}
	}
	op->children[0] = GeneratePullupFilter(std::move(op->children[0]), left_filters);
	return ReturnConstraints(std::move(op), std::move(constraints));
}

unique_ptr<LogicalOperator> FilterPullup::PullupInnerJoin(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->Cast<LogicalJoin>().join_type == JoinType::INNER);
	if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		return op;
	}
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_ANY_JOIN);

	// Get the filters from both sides of the join
	op = PullupBothSide(std::move(op));
	vector<unique_ptr<Expression>> expressions;
	if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		expressions = std::move(op->expressions);
		op = std::move(op->children[0]);
	} else if (!can_pullup) {
		return op; // No filters from below, and we can't pullup, stop.
	}

	// Also extract the filters of the joins
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &comp_join = op->Cast<LogicalComparisonJoin>();
		for (auto &cond : comp_join.conditions) {
			expressions.push_back(JoinCondition::CreateExpression(std::move(cond)));
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &any_join = op->Cast<LogicalAnyJoin>();
		expressions.push_back(std::move(any_join.condition));
		break;
	}
	default:
		throw NotImplementedException("PullupInnerJoin for LogicalOperatorType::%s", EnumUtil::ToString(op->type));
	}

	// Convert to cross product
	op = make_uniq<LogicalCrossProduct>(std::move(op->children[0]), std::move(op->children[1]));
	if (can_pullup) {
		for (auto &expr : expressions) {
			filters_expr_pullup.push_back(std::move(expr));
		}
	} else {
		op = GeneratePullupFilter(std::move(op), expressions);
	}
	return op;
}

unique_ptr<LogicalOperator> FilterPullup::PullupCrossProduct(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT);
	return PullupBothSide(std::move(op));
}

unique_ptr<LogicalOperator> FilterPullup::GeneratePullupFilter(unique_ptr<LogicalOperator> child,
                                                               vector<unique_ptr<Expression>> &expressions) {
	if (expressions.empty()) {
		return child;
	}
	FilterCombiner::DeduplicateFilters(expressions);
	unique_ptr<LogicalFilter> filter = make_uniq<LogicalFilter>();
	for (idx_t i = 0; i < expressions.size(); ++i) {
		filter->expressions.push_back(std::move(expressions[i]));
	}
	expressions.clear();
	filter->children.push_back(std::move(child));
	return std::move(filter);
}

unique_ptr<LogicalOperator> FilterPullup::FinishPullup(unique_ptr<LogicalOperator> op) {
	// unhandled type, first perform filter pushdown in its children
	for (idx_t i = 0; i < op->children.size(); i++) {
		FilterPullup pullup(context);
		op->children[i] = pullup.Rewrite(std::move(op->children[i]));
	}
	// now pull up any existing filters
	if (filters_expr_pullup.empty()) {
		// no filters to pull up
		return op;
	}
	return GeneratePullupFilter(std::move(op), filters_expr_pullup);
}

} // namespace duckdb
