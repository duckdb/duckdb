#include "duckdb/optimizer/rule/distinct_aggregate_optimizer.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

namespace duckdb {

DistinctAggregateOptimizer::DistinctAggregateOptimizer(ExpressionRewriter &rewriter) : Rule(rewriter) {
	root = make_uniq<ExpressionMatcher>();
	root->expr_class = ExpressionClass::BOUND_AGGREGATE;
}

unique_ptr<Expression> DistinctAggregateOptimizer::Apply(ClientContext &context, BoundAggregateExpression &aggr,
                                                         bool &changes_made) {
	if (!aggr.IsDistinct()) {
		// no DISTINCT defined
		return nullptr;
	}
	if (aggr.function.distinct_dependent == AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT) {
		// not a distinct-sensitive aggregate but we have an DISTINCT modifier - remove it
		aggr.aggr_type = AggregateType::NON_DISTINCT;
		changes_made = true;
		return nullptr;
	}
	return nullptr;
}

unique_ptr<Expression> DistinctAggregateOptimizer::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                         bool &changes_made, bool is_root) {
	auto &aggr = bindings[0].get().Cast<BoundAggregateExpression>();
	return Apply(rewriter.context, aggr, changes_made);
}

DistinctWindowedOptimizer::DistinctWindowedOptimizer(ExpressionRewriter &rewriter) : Rule(rewriter) {
	root = make_uniq<ExpressionMatcher>();
	root->expr_class = ExpressionClass::BOUND_WINDOW;
}

unique_ptr<Expression> DistinctWindowedOptimizer::Apply(ClientContext &context, BoundWindowExpression &wexpr,
                                                        bool &changes_made) {
	if (!wexpr.distinct) {
		// no DISTINCT defined
		return nullptr;
	}
	if (!wexpr.aggregate) {
		// not an aggregate
		return nullptr;
	}
	if (wexpr.aggregate->distinct_dependent == AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT) {
		// not a distinct-sensitive aggregate but we have an DISTINCT modifier - remove it
		wexpr.distinct = false;
		changes_made = true;
		return nullptr;
	}
	return nullptr;
}

unique_ptr<Expression> DistinctWindowedOptimizer::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                        bool &changes_made, bool is_root) {
	auto &wexpr = bindings[0].get().Cast<BoundWindowExpression>();
	return Apply(rewriter.context, wexpr, changes_made);
}

} // namespace duckdb
