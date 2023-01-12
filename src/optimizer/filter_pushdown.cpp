#include "duckdb/optimizer/filter_pushdown.hpp"

#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

FilterPushdown::FilterPushdown(Optimizer &optimizer) : optimizer(optimizer), combiner(optimizer.context) {
}

unique_ptr<LogicalOperator> FilterPushdown::Rewrite(unique_ptr<LogicalOperator> op) {
	D_ASSERT(!combiner.HasFilters());
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return PushdownAggregate(std::move(op));
	case LogicalOperatorType::LOGICAL_FILTER:
		return PushdownFilter(std::move(op));
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return PushdownCrossProduct(std::move(op));
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		return PushdownJoin(std::move(op));
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return PushdownProjection(std::move(op));
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_UNION:
		return PushdownSetOperation(std::move(op));
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		// we can just push directly through these operations without any rewriting
		op->children[0] = Rewrite(std::move(op->children[0]));
		return op;
	}
	case LogicalOperatorType::LOGICAL_GET:
		return PushdownGet(std::move(op));
	case LogicalOperatorType::LOGICAL_LIMIT:
		return PushdownLimit(std::move(op));
	default:
		return FinishPushdown(std::move(op));
	}
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownJoin(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_ANY_JOIN || op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
	auto &join = (LogicalJoin &)*op;
	unordered_set<idx_t> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op->children[1], right_bindings);

	switch (join.join_type) {
	case JoinType::INNER:
		return PushdownInnerJoin(std::move(op), left_bindings, right_bindings);
	case JoinType::LEFT:
		return PushdownLeftJoin(std::move(op), left_bindings, right_bindings);
	case JoinType::MARK:
		return PushdownMarkJoin(std::move(op), left_bindings, right_bindings);
	case JoinType::SINGLE:
		return PushdownSingleJoin(std::move(op), left_bindings, right_bindings);
	default:
		// unsupported join type: stop pushing down
		return FinishPushdown(std::move(op));
	}
}
void FilterPushdown::PushFilters() {
	for (auto &f : filters) {
		auto result = combiner.AddFilter(std::move(f->filter));
		D_ASSERT(result != FilterResult::UNSUPPORTED);
		(void)result;
	}
	filters.clear();
}

FilterResult FilterPushdown::AddFilter(unique_ptr<Expression> expr) {
	PushFilters();
	// split up the filters by AND predicate
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(std::move(expr));
	LogicalFilter::SplitPredicates(expressions);
	// push the filters into the combiner
	for (auto &child_expr : expressions) {
		if (combiner.AddFilter(std::move(child_expr)) == FilterResult::UNSATISFIABLE) {
			return FilterResult::UNSATISFIABLE;
		}
	}
	return FilterResult::SUCCESS;
}

void FilterPushdown::GenerateFilters() {
	if (!filters.empty()) {
		D_ASSERT(!combiner.HasFilters());
		return;
	}
	combiner.GenerateFilters([&](unique_ptr<Expression> filter) {
		auto f = make_unique<Filter>();
		f->filter = std::move(filter);
		f->ExtractBindings();
		filters.push_back(std::move(f));
	});
}

unique_ptr<LogicalOperator> FilterPushdown::FinishPushdown(unique_ptr<LogicalOperator> op) {
	// unhandled type, first perform filter pushdown in its children
	for (auto &child : op->children) {
		FilterPushdown pushdown(optimizer);
		child = pushdown.Rewrite(std::move(child));
	}
	// now push any existing filters
	if (filters.empty()) {
		// no filters to push
		return op;
	}
	auto filter = make_unique<LogicalFilter>();
	for (auto &f : filters) {
		filter->expressions.push_back(std::move(f->filter));
	}
	filter->children.push_back(std::move(op));
	return std::move(filter);
}

void FilterPushdown::Filter::ExtractBindings() {
	bindings.clear();
	LogicalJoin::GetExpressionBindings(*filter, bindings);
}

} // namespace duckdb
