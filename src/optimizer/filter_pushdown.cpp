#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

void FilterPushdown::CheckMarkToSemi(LogicalOperator &op, unordered_set<idx_t> &table_bindings) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (join.join_type != JoinType::MARK) {
			break;
		}
		// if the projected table bindings include the mark join index,
		if (table_bindings.find(join.mark_index) != table_bindings.end()) {
			join.convert_mark_to_semi = false;
		}
		break;
	}
	// you need to store table.column index.
	// if you get to a projection, you need to change the table_bindings passed so they reflect the
	// table index of the original expression they originated from.
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// when we encounter a projection, replace the table_bindings with
		// the tables in the projection
		auto plan_bindings = op.GetColumnBindings();
		auto &proj = op.Cast<LogicalProjection>();
		auto proj_bindings = proj.GetColumnBindings();
		unordered_set<idx_t> new_table_bindings;
		for (auto &binding : proj_bindings) {
			auto col_index = binding.column_index;
			auto &expr = proj.expressions.at(col_index);
			vector<ColumnBinding> bindings_to_keep;
			ExpressionIterator::EnumerateExpression(expr, [&](Expression &child) {
				if (expr->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
					auto &col_ref = expr->Cast<BoundColumnRefExpression>();
					bindings_to_keep.push_back(col_ref.binding);
				}
			});
			for (auto &expr_binding : bindings_to_keep) {
				new_table_bindings.insert(expr_binding.table_index);
			}
			table_bindings = new_table_bindings;
		}
		break;
	}
	default:
		break;
	}

	// recurse into the children to find mark joins and project their indexes.
	for (auto &child : op.children) {
		CheckMarkToSemi(*child, table_bindings);
	}
}

FilterPushdown::FilterPushdown(Optimizer &optimizer, bool convert_mark_joins)
    : optimizer(optimizer), combiner(optimizer.context), convert_mark_joins(convert_mark_joins) {
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
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		// we can just push directly through these operations without any rewriting
		op->children[0] = Rewrite(std::move(op->children[0]));
		return op;
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: {
		// we can't push filters into the materialized CTE (LHS), but we do want to recurse into it
		FilterPushdown pushdown(optimizer, convert_mark_joins);
		op->children[0] = pushdown.Rewrite(std::move(op->children[0]));
		// we can push filters into the rest of the query plan (RHS)
		op->children[1] = Rewrite(std::move(op->children[1]));
		return op;
	}
	case LogicalOperatorType::LOGICAL_GET:
		return PushdownGet(std::move(op));
	case LogicalOperatorType::LOGICAL_LIMIT:
		return PushdownLimit(std::move(op));
	case LogicalOperatorType::LOGICAL_WINDOW:
		return PushdownWindow(std::move(op));
	case LogicalOperatorType::LOGICAL_UNNEST:
		return PushdownUnnest(std::move(op));
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
	if (join.HasProjectionMap()) {
		// cannot push down further otherwise the projection maps won't be preserved
		return FinishPushdown(std::move(op));
	}

	unordered_set<idx_t> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op->children[1], right_bindings);

	switch (join.join_type) {
	case JoinType::INNER:
		//	AsOf joins can't push anything into the RHS, so treat it as a left join
		if (op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
			return PushdownLeftJoin(std::move(op), left_bindings, right_bindings);
		}
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
	if (op->has_estimated_cardinality) {
		// set the filter's estimated cardinality as the child op's.
		// if the filter is created during the filter pushdown optimization, the estimated cardinality will be later
		// overridden during the join order optimization to a more accurate one.
		// if the filter is created during the statistics propagation, the estimated cardinality won't be set unless set
		// here. assuming the filters introduced during the statistics propagation have little effect in reducing the
		// cardinality, we adopt the the cardinality of the child. this could be improved by MinMax info from the
		// statistics propagation
		filter->SetEstimatedCardinality(op->estimated_cardinality);
	}
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
		FilterPushdown pushdown(optimizer, convert_mark_joins);
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
