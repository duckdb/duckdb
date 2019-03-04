#include "optimizer/filter_pushdown.hpp"

#include "planner/operator/list.hpp"
#include "execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::Rewrite(unique_ptr<LogicalOperator> op) {
	switch (op->type) {
	case LogicalOperatorType::FILTER:
		return PushdownFilter(move(op));
	case LogicalOperatorType::CROSS_PRODUCT:
		return PushdownCrossProduct(move(op));
	case LogicalOperatorType::COMPARISON_JOIN:
	case LogicalOperatorType::ANY_JOIN:
	case LogicalOperatorType::DELIM_JOIN:
		return PushdownJoin(move(op));
	case LogicalOperatorType::SUBQUERY:
		return PushdownSubquery(move(op));
	case LogicalOperatorType::PROJECTION:
		return PushdownProjection(move(op));
	default:
		return FinishPushdown(move(op));
	}
}

bool FilterPushdown::AddFilter(unique_ptr<Expression> expr) {
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(move(expr));
	LogicalFilter::SplitPredicates(expressions);
	for(auto &expr : expressions) {
		auto f = make_unique<Filter>();
		f->filter = move(expr);
		LogicalJoin::GetExpressionBindings(*f->filter, f->bindings);
		if (f->bindings.size() == 0) {
			// scalar condition, evaluate it
			auto result = ExpressionExecutor::EvaluateScalar(*f->filter).CastAs(TypeId::BOOLEAN);
			// check if the filter passes
			if (result.is_null || !result.value_.boolean) {
				// the filter does not pass the scalar test, create an empty result
				return true;
			} else {
				// the filter passes the scalar test, just remove the condition
				continue;
			}
		}
		filters.push_back(move(f));
	}
	return false;
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownJoin(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::COMPARISON_JOIN || op->type == LogicalOperatorType::ANY_JOIN || op->type == LogicalOperatorType::DELIM_JOIN);
	auto &join = (LogicalJoin &)*op;
	unordered_set<size_t> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op->children[1], right_bindings);

	// inner join should not occur here
	// because explicit inner joins are turned into cross product + filters and then transformed into inner joins in the
	// filter pushdown phase
	switch (join.type) {
	case JoinType::INNER:
		return PushdownInnerJoin(move(op), left_bindings, right_bindings);
	case JoinType::LEFT:
		return PushdownLeftJoin(move(op), left_bindings, right_bindings);
	case JoinType::MARK:
		return PushdownMarkJoin(move(op), left_bindings, right_bindings);
	case JoinType::SINGLE:
		return PushdownSingleJoin(move(op), left_bindings, right_bindings);
	default:
		// unsupported join type: stop pushing down
		return FinishPushdown(move(op));
	}
}

unique_ptr<LogicalOperator> FilterPushdown::FinishPushdown(unique_ptr<LogicalOperator> op) {
	// unhandled type, first perform filter pushdown in its children
	for (size_t i = 0; i < op->children.size(); i++) {
		FilterPushdown pushdown;
		op->children[i] = pushdown.Rewrite(move(op->children[i]));
	}
	// now push any existing filters
	if (filters.size() == 0) {
		// no filters to push
		return op;
	}
	auto filter = make_unique<LogicalFilter>();
	for (auto &f : filters) {
		filter->expressions.push_back(move(f->filter));
	}
	filter->children.push_back(move(op));
	return move(filter);
}
