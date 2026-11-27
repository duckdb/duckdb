#include "duckdb/optimizer/aggregate_filter_extractor.hpp"

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/removed_subtree_effects.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/expression_barrier.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

static unique_ptr<Expression> CreateInputFilter(unique_ptr<Expression> predicate) {
	optional_ptr<const BoundColumnRefExpression> column;
	bool single_column = true;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(*predicate, [&](const BoundColumnRefExpression &ref) {
		if (!column) {
			column = ref;
		} else if (column->Binding() != ref.Binding() || column->Depth() != ref.Depth()) {
			single_column = false;
		}
	});
	if (!single_column) {
		return nullptr;
	}
	if (!column || predicate->GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
		return predicate;
	}
	// Redundant disjunctions can prune row groups without adding per-row evaluation.
	auto input = column->Copy();
	BoundReferenceExpression placeholder(input->GetReturnType(), 0ULL);
	ExpressionFilter::ReplaceExpressionRecursive(predicate, placeholder, ExpressionType::BOUND_COLUMN_REF);
	ExpressionFilter filter(CreateOptionalFilterExpression(std::move(predicate), input->GetReturnType()));
	return filter.ToExpression(*input);
}

AggregateFilterExtractor::AggregateFilterExtractor(Optimizer &optimizer, LogicalAggregate &aggr)
    : optimizer(optimizer), aggr(aggr) {
	if (optimizer.OptimizerDisabled(OptimizerType::FILTER_PUSHDOWN) || !aggr.groups.empty() ||
	    aggr.grouping_sets.size() > 1) {
		can_filter_input = false;
	}
}

unique_ptr<Expression> AggregateFilterExtractor::Extract(const vector<idx_t> &aggregate_indices) {
	if ((can_filter_input.has_value() && !can_filter_input.value()) || aggregate_indices.empty()) {
		return nullptr;
	}
	expression_set_t seen;
	vector<unique_ptr<Expression>> predicates;
	for (auto aggregate_idx : aggregate_indices) {
		auto &aggregate = aggr.expressions[aggregate_idx]->Cast<BoundAggregateExpression>();
		auto &filter = aggregate.GetFilter();
		if (!filter || aggregate.IsVolatile() || ExpressionBarrier::Contains(*filter)) {
			return nullptr;
		}
		bool can_throw = false;
		ExpressionIterator::EnumerateChildren(aggregate,
		                                      [&](const Expression &child) { can_throw |= child.CanThrow(); });
		if (can_throw) {
			return nullptr;
		}
		if (seen.insert(*filter).second) {
			predicates.push_back(filter->Copy());
		}
	}
	const bool shared_filter = predicates.size() == 1;
	LogicalFilter rewritten;
	rewritten.expressions.push_back(
	    BoundConjunctionExpression::Create(ExpressionType::CONJUNCTION_OR, std::move(predicates)));
	if (!optimizer.OptimizerDisabled(OptimizerType::EXPRESSION_REWRITER)) {
		optimizer.rewriter.VisitOperator(rewritten);
	}
	if (!shared_filter) {
		vector<unique_ptr<Expression>> input_filters;
		for (auto &predicate : rewritten.expressions) {
			auto input_filter = CreateInputFilter(std::move(predicate));
			if (input_filter) {
				input_filters.push_back(std::move(input_filter));
			}
		}
		rewritten.expressions = std::move(input_filters);
	}
	if (rewritten.expressions.empty()) {
		return nullptr;
	}
	if (!can_filter_input.has_value()) {
		RemovedSubtreeEffects effects(*aggr.children[0]);
		can_filter_input = !effects.HasEffect(*aggr.children[0]);
	}
	if (!can_filter_input.value()) {
		return nullptr;
	}
	if (shared_filter) {
		for (auto aggregate_idx : aggregate_indices) {
			aggr.expressions[aggregate_idx]->Cast<BoundAggregateExpression>().GetFilterMutable().reset();
		}
	}
	return BoundConjunctionExpression::Create(ExpressionType::CONJUNCTION_AND, std::move(rewritten.expressions));
}

} // namespace duckdb
