#include "duckdb/optimizer/rule/ordered_aggregate_optimizer.hpp"

#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

OrderedAggregateOptimizer::OrderedAggregateOptimizer(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// we match on an OR expression within a LogicalFilter node
	root = make_uniq<ExpressionMatcher>();
	root->expr_class = ExpressionClass::BOUND_AGGREGATE;
}

unique_ptr<Expression> OrderedAggregateOptimizer::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                        bool &changes_made, bool is_root) {
	auto &aggr = bindings[0].get().Cast<BoundAggregateExpression>();
	if (!aggr.order_bys) {
		// no ORDER BYs defined
		return nullptr;
	}
	if (aggr.function.order_dependent == AggregateOrderDependent::NOT_ORDER_DEPENDENT) {
		// not an order dependent aggregate but we have an ORDER BY clause - remove it
		aggr.order_bys.reset();
		changes_made = true;
		return nullptr;
	}
	return nullptr;
}

} // namespace duckdb
