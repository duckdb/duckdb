#include "duckdb/optimizer/sum_rewriter.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {

unique_ptr<SetTypesMatcher> GetSmallIntegerTypesMatcher() {
	vector<LogicalType> types {LogicalTypeId::TINYINT,  LogicalTypeId::SMALLINT, LogicalTypeId::INTEGER,
	                           LogicalTypeId::BIGINT,   LogicalTypeId::UTINYINT, LogicalTypeId::USMALLINT,
	                           LogicalTypeId::UINTEGER, LogicalTypeId::UBIGINT};
	return make_uniq<SetTypesMatcher>(std::move(types));
}

SumRewriterOptimizer::SumRewriterOptimizer(Optimizer &optimizer) : optimizer(optimizer) {
	// set up an expression matcher that detects SUM(x + C) or SUM(C + x)
	auto op = make_uniq<AggregateExpressionMatcher>();
	op->function = make_uniq<SpecificFunctionMatcher>("sum");
	op->policy = SetMatcher::Policy::UNORDERED;

	auto arithmetic = make_uniq<FunctionExpressionMatcher>();
	// handle X + C where
	arithmetic->function = make_uniq<SpecificFunctionMatcher>("+");
	// we match only on integral numeric types
	arithmetic->type = make_uniq<IntegerTypeMatcher>();
	auto child_constant_matcher = make_uniq<ConstantExpressionMatcher>();
	auto child_expression_matcher = make_uniq<StableExpressionMatcher>();
	child_constant_matcher->type = GetSmallIntegerTypesMatcher();
	child_expression_matcher->type = GetSmallIntegerTypesMatcher();
	arithmetic->matchers.push_back(std::move(child_constant_matcher));
	arithmetic->matchers.push_back(std::move(child_expression_matcher));
	arithmetic->policy = SetMatcher::Policy::SOME;
	op->matchers.push_back(std::move(arithmetic));

	sum_matcher = std::move(op);
}

SumRewriterOptimizer::~SumRewriterOptimizer() {
}

void SumRewriterOptimizer::Optimize(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		RewriteSums(op);
	}
	VisitOperator(*op);
}

void SumRewriterOptimizer::StandardVisitOperator(LogicalOperator &op) {
	for (auto &child : op.children) {
		Optimize(child);
	}
	if (!aggregate_map.empty()) {
		VisitOperatorExpressions(op);
	}
}

void SumRewriterOptimizer::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		SumRewriterOptimizer sum_rewriter(optimizer);
		sum_rewriter.StandardVisitOperator(op);
		return;
	}
	default:
		break;
	}

	StandardVisitOperator(op);
}

unique_ptr<Expression> SumRewriterOptimizer::VisitReplace(BoundColumnRefExpression &expr,
                                                          unique_ptr<Expression> *expr_ptr) {
	// check if this column ref points to an aggregate that was remapped; if it does we remap it
	auto entry = aggregate_map.find(expr.binding);
	if (entry != aggregate_map.end()) {
		expr.binding = entry->second;
	}
	return nullptr;
}

void SumRewriterOptimizer::RewriteSums(unique_ptr<LogicalOperator> &op) {
	auto &aggr = op->Cast<LogicalAggregate>();
	if (!aggr.groups.empty()) {
		return;
	}

	unordered_set<idx_t> rewrote_map;
	vector<unique_ptr<Expression>> constants;
	idx_t aggr_count = aggr.expressions.size();
	for (idx_t i = 0; i < aggr_count; ++i) {
		auto &expr = aggr.expressions[i];
		vector<reference<Expression>> bindings;
		if (!sum_matcher->Match(*expr, bindings)) {
			continue;
		}
		// found SUM(x + C)
		auto &sum = bindings[0].get().Cast<BoundAggregateExpression>();
		auto &addition = bindings[1].get().Cast<BoundFunctionExpression>();
		idx_t const_idx = addition.children[0]->GetExpressionType() == ExpressionType::VALUE_CONSTANT ? 0 : 1;
		auto const_expr = std::move(addition.children[const_idx]);
		auto main_expr = std::move(addition.children[1 - const_idx]);

		// turn this into SUM(x)
		sum.children[0] = main_expr->Copy();

		// push a new aggregate - COUNT(x)
		FunctionBinder function_binder(optimizer.context);

		auto count_fun = CountFunctionBase::GetFunction();
		vector<unique_ptr<Expression>> children;
		children.push_back(std::move(main_expr));
		auto count_aggr =
		    function_binder.BindAggregateFunction(count_fun, std::move(children), nullptr, AggregateType::NON_DISTINCT);
		aggr.expressions.push_back(std::move(count_aggr));
		constants.push_back(std::move(const_expr));
		rewrote_map.insert(i);
	}
	if (rewrote_map.empty()) {
		return;
	}
	vector<unique_ptr<Expression>> projection_expressions;
	// we rewrote aggregates - we need to push a projection in which we re-compute the original result
	idx_t rewritten_index = 0;
	auto proj_index = optimizer.binder.GenerateTableIndex();
	for (idx_t i = 0; i < aggr_count; i++) {
		ColumnBinding aggregate_binding(aggr.aggregate_index, i);
		aggregate_map[aggregate_binding] = ColumnBinding(proj_index, i);
		auto &aggr_type = aggr.expressions[i]->return_type;
		auto aggr_ref = make_uniq<BoundColumnRefExpression>(aggr_type, aggregate_binding);
		if (rewrote_map.find(i) == rewrote_map.end()) {
			// not rewritten - just push a reference
			projection_expressions.push_back(std::move(aggr_ref));
			continue;
		}
		// rewritten - need to compute the final result
		idx_t count_idx = aggr_count + rewritten_index;
		ColumnBinding count_binding(aggr.aggregate_index, count_idx);
		auto count_ref = make_uniq<BoundColumnRefExpression>(aggr.expressions[count_idx]->return_type, count_binding);

		// cast the count to the sum type
		auto cast_count = BoundCastExpression::AddCastToType(optimizer.context, std::move(count_ref), aggr_type);
		auto const_expr =
		    BoundCastExpression::AddCastToType(optimizer.context, std::move(constants[rewritten_index]), aggr_type);

		// bind the multiplication
		auto multiply = optimizer.BindScalarFunction("*", std::move(cast_count), std::move(const_expr));

		// add it to the sum
		auto final_result = optimizer.BindScalarFunction("+", std::move(aggr_ref), std::move(multiply));
		projection_expressions.push_back(std::move(final_result));

		rewritten_index++;
	}

	// push the projection to replace the aggregate
	auto proj = make_uniq<LogicalProjection>(proj_index, std::move(projection_expressions));
	proj->children.push_back(std::move(op));
	op = std::move(proj);
}

} // namespace duckdb
