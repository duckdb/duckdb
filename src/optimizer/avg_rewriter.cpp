#include "duckdb/optimizer/avg_rewriter.hpp"

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
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"

namespace duckdb {

AvgRewriterOptimizer::AvgRewriterOptimizer(Optimizer &optimizer) : optimizer(optimizer) {
	// Set up an expression matcher that detects AVG(x)
	auto op = make_uniq<AggregateExpressionMatcher>();
	op->function = make_uniq<SpecificFunctionMatcher>("avg");
	op->policy = SetMatcher::Policy::ORDERED;
	op->matchers.push_back(make_uniq<ExpressionMatcher>());
	avg_matcher = std::move(op);
}

AvgRewriterOptimizer::~AvgRewriterOptimizer() {
}

void AvgRewriterOptimizer::Optimize(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		RewriteAvgs(op);
	}
	VisitOperator(*op);
}

void AvgRewriterOptimizer::StandardVisitOperator(LogicalOperator &op) {
	for (auto &child : op.children) {
		Optimize(child);
	}
	if (!aggregate_map.empty()) {
		VisitOperatorExpressions(op);
	}
}

void AvgRewriterOptimizer::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		AvgRewriterOptimizer avg_rewriter(optimizer);
		avg_rewriter.StandardVisitOperator(op);
		return;
	}
	default:
		break;
	}

	StandardVisitOperator(op);
}

unique_ptr<Expression> AvgRewriterOptimizer::VisitReplace(BoundColumnRefExpression &expr,
                                                          unique_ptr<Expression> *expr_ptr) {
	// Check if this column ref points to an aggregate that was remapped; if it does we remap it
	const auto entry = aggregate_map.find(expr.binding);
	if (entry != aggregate_map.end()) {
		expr.binding = entry->second;
	}
	return nullptr;
}

void AvgRewriterOptimizer::RewriteAvgs(unique_ptr<LogicalOperator> &op) {
	auto &aggr = op->Cast<LogicalAggregate>();
	if (!aggr.grouping_functions.empty() || aggr.grouping_sets.size() > 1) {
		return;
	}
	const idx_t aggr_count = aggr.expressions.size();

	auto &catalog = Catalog::GetSystemCatalog(optimizer.context);
	FunctionBinder function_binder(optimizer.context);

	// Rewrite all AVG(x) to SUM(x), and add a COUNT(x) to the list of aggregates
	unordered_map<idx_t, idx_t> rewrote_map;
	for (idx_t i = 0; i < aggr_count; ++i) {
		auto &expr = aggr.expressions[i];
		vector<reference<Expression>> bindings;
		if (!avg_matcher->Match(*expr, bindings)) {
			continue;
		}

		// Found AVG(x), turn it into SUM(x)
		auto avg_child = std::move(bindings[0].get().Cast<BoundAggregateExpression>().children[0]);
		auto &sum_entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(optimizer.context, DEFAULT_SCHEMA, "sum");
		const auto sum_fun = sum_entry.functions.GetFunctionByArguments(optimizer.context, {avg_child->return_type});
		vector<unique_ptr<Expression>> args;
		args.push_back(std::move(avg_child));
		avg_child = args.back()->Copy();
		expr = function_binder.BindAggregateFunction(sum_fun, std::move(args));

		// Map from SUM(x) index to COUNT(x) index
		rewrote_map.emplace(i, aggr.expressions.size());

		// Create COUNT(x)
		const auto count_fun = CountFunctionBase::GetFunction();
		args = vector<unique_ptr<Expression>>();
		args.push_back(std::move(avg_child));
		auto count_aggr =
		    function_binder.BindAggregateFunction(count_fun, std::move(args), nullptr, AggregateType::NON_DISTINCT);
		aggr.expressions.push_back(std::move(count_aggr));
	}

	if (rewrote_map.empty()) {
		return;
	}

	// We rewrote aggregates - we need to push a projection in which we re-compute the original result
	auto proj_index = optimizer.binder.GenerateTableIndex();
	const auto group_count = aggr.groups.size();
	vector<unique_ptr<Expression>> projection_expressions;
	for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
		ColumnBinding aggregate_binding(aggr.group_index, group_idx);
		aggregate_map[aggregate_binding] = ColumnBinding(proj_index, group_idx);
		auto group_ref = make_uniq<BoundColumnRefExpression>(aggr.groups[group_idx]->return_type, aggregate_binding);
		projection_expressions.push_back(std::move(group_ref));
	}

	for (idx_t i = 0; i < aggr_count; i++) {
		ColumnBinding aggregate_binding(aggr.aggregate_index, i);
		aggregate_map[aggregate_binding] = ColumnBinding(proj_index, group_count + i);
		auto &aggr_type = aggr.expressions[i]->return_type;
		auto aggr_ref = make_uniq<BoundColumnRefExpression>(aggr_type, aggregate_binding);

		const auto rewrote_entry = rewrote_map.find(i);
		if (rewrote_entry == rewrote_map.end()) {
			// Not rewritten - just push a reference
			projection_expressions.push_back(std::move(aggr_ref));
			continue;
		}

		// Rewritten - need to compute the final result by dividing SUM(x) by COUNT(x)
		ColumnBinding count_binding(aggr.aggregate_index, rewrote_entry->second);
		auto count_ref =
		    make_uniq<BoundColumnRefExpression>(aggr.expressions[rewrote_entry->second]->return_type, count_binding);

		auto final_result = optimizer.BindScalarFunction("/", std::move(aggr_ref), std::move(count_ref));
		projection_expressions.push_back(std::move(final_result));
	}

	// Push the projection to replace the aggregate
	auto proj = make_uniq<LogicalProjection>(proj_index, std::move(projection_expressions));
	if (op->has_estimated_cardinality) {
		proj->SetEstimatedCardinality(op->estimated_cardinality);
	}
	proj->children.push_back(std::move(op));
	op = std::move(proj);
}

} // namespace duckdb
