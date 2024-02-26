#include "duckdb/optimizer/filter_pushdown.hpp"

#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

FilterPushdown::FilterPushdown(Optimizer &optimizer) : optimizer(optimizer), combiner(optimizer.context) {
}

static bool FilterIsOnPartition(column_binding_set_t partition_bindings, Expression &filter_expression) {
	bool filter_is_on_partition = false;
	ExpressionIterator::EnumerateChildren(filter_expression, [&](unique_ptr<Expression> &child) {
		switch (child->type) {
		case ExpressionType::BOUND_COLUMN_REF: {
			auto &col_ref = child->Cast<BoundColumnRefExpression>();
			if (partition_bindings.find(col_ref.binding) != partition_bindings.end()) {
				filter_is_on_partition = true;
				return;
			}
			break;
		}
		case ExpressionType::CONJUNCTION_AND:
		case ExpressionType::CONJUNCTION_OR: {
			filter_is_on_partition = false;
			return;
		}
		default:
			break;
		}
	});
	return filter_is_on_partition;
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownWindow(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_WINDOW);
	auto &window = op->Cast<LogicalWindow>();
	// go into expressions, then look into the partitions.
	// if the filter applies to a partition in each window expression then you can push the filter
	// into the children.
	auto pushdown = FilterPushdown(optimizer);
	vector<int8_t> filters_to_pushdown;
	filters_to_pushdown.reserve(filters.size());
	for (idx_t i = 0; i < filters.size(); i++) {
		filters_to_pushdown.push_back(0);
	}
	// 1. Check every window expression
	for (auto &expr : window.expressions) {
		if (expr->expression_class != ExpressionClass::BOUND_WINDOW) {
			continue;
		}
		auto &window_expr = expr->Cast<BoundWindowExpression>();
		auto &partitions = window_expr.partitions;
		column_binding_set_t parition_bindings;
		// 2. Get the binding information of the partitions
		for (auto &partition_expr : partitions) {
			switch (partition_expr->type) {
			// TODO: Add expressions for function expressions like FLOOR, CEIL etc.
			case ExpressionType::BOUND_COLUMN_REF: {
				auto &partition_col = partition_expr->Cast<BoundColumnRefExpression>();
				parition_bindings.insert(partition_col.binding);
				break;
			}
			default:
				break;
			}
		}

		// 3. Check if a filter is on one of the partitions
		//    in the event there are multiple window functions, a fiter can only be pushed down
		//    if each window function is partitioned on the filtered value
		//    if a filter is not on a partition fiters_to_parition[filter_index] = -1
		//    which means the filter will no longer be pushed down, even if the filter is on a partition.
		//    tpcds q47 caught this
		for (idx_t i = 0; i < filters.size(); i++) {
			auto &filter = filters.at(i);
			if (FilterIsOnPartition(parition_bindings, *filter->filter) && filters_to_pushdown[i] >= 0) {
				filters_to_pushdown[i] = 1;
				continue;
			}
			filters_to_pushdown[i] = -1;
		}
	}
	// push the new filters into the child.
	vector<unique_ptr<Filter>> leftover_filters;
	for (idx_t i = 0; i < filters.size(); i++) {
		if (filters_to_pushdown[i] == 1) {
			pushdown.filters.push_back(std::move(filters.at(i)));
			continue;
		}
		leftover_filters.push_back(std::move(filters.at(i)));
	}
	if (!pushdown.filters.empty()) {
		op->children[0] = pushdown.Rewrite(std::move(op->children[0]));
	}
	filters = std::move(leftover_filters);

	return FinishPushdown(std::move(op));
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
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		return PushdownJoin(std::move(op));
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return PushdownProjection(std::move(op));
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_UNION:
		return PushdownSetOperation(std::move(op));
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return PushdownDistinct(std::move(op));
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		// we can just push directly through these operations without any rewriting
		op->children[0] = Rewrite(std::move(op->children[0]));
		return op;
	}
	case LogicalOperatorType::LOGICAL_GET:
		return PushdownGet(std::move(op));
	case LogicalOperatorType::LOGICAL_LIMIT:
		return PushdownLimit(std::move(op));
	case LogicalOperatorType::LOGICAL_WINDOW:
		return PushdownWindow(std::move(op));
	default:
		return FinishPushdown(std::move(op));
	}
}

ClientContext &FilterPushdown::GetContext() {
	return optimizer.GetContext();
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownJoin(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN || op->type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
	auto &join = op->Cast<LogicalJoin>();
	if (!join.left_projection_map.empty() || !join.right_projection_map.empty()) {
		// cannot push down further otherwise the projection maps won't be preserved
		return FinishPushdown(std::move(op));
	}

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
	case JoinType::SEMI:
	case JoinType::ANTI:
		return PushdownSemiAntiJoin(std::move(op));
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
		auto f = make_uniq<Filter>();
		f->filter = std::move(filter);
		f->ExtractBindings();
		filters.push_back(std::move(f));
	});
}

unique_ptr<LogicalOperator> FilterPushdown::AddLogicalFilter(unique_ptr<LogicalOperator> op,
                                                             vector<unique_ptr<Expression>> expressions) {
	if (expressions.empty()) {
		// No left expressions, so needn't to add an extra filter operator.
		return op;
	}
	auto filter = make_uniq<LogicalFilter>();
	filter->expressions = std::move(expressions);
	filter->children.push_back(std::move(op));
	return std::move(filter);
}

unique_ptr<LogicalOperator> FilterPushdown::PushFinalFilters(unique_ptr<LogicalOperator> op) {
	vector<unique_ptr<Expression>> expressions;
	for (auto &f : filters) {
		expressions.push_back(std::move(f->filter));
	}

	return AddLogicalFilter(std::move(op), std::move(expressions));
}

unique_ptr<LogicalOperator> FilterPushdown::FinishPushdown(unique_ptr<LogicalOperator> op) {
	// unhandled type, first perform filter pushdown in its children
	for (auto &child : op->children) {
		FilterPushdown pushdown(optimizer);
		child = pushdown.Rewrite(std::move(child));
	}
	// now push any existing filters
	return PushFinalFilters(std::move(op));
}

void FilterPushdown::Filter::ExtractBindings() {
	bindings.clear();
	LogicalJoin::GetExpressionBindings(*filter, bindings);
}

} // namespace duckdb
