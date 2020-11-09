#include "duckdb/optimizer/filter_pushdown.hpp"

#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {
using namespace std;

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::Rewrite(unique_ptr<LogicalOperator> op) {
	D_ASSERT(!combiner.HasFilters());
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return PushdownAggregate(move(op));
	case LogicalOperatorType::LOGICAL_FILTER:
		return PushdownFilter(move(op));
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return PushdownCrossProduct(move(op));
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		return PushdownJoin(move(op));
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return PushdownProjection(move(op));
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_UNION:
		return PushdownSetOperation(move(op));
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		// we can just push directly through these operations without any rewriting
		op->children[0] = Rewrite(move(op->children[0]));
		return op;
	}
	case LogicalOperatorType::LOGICAL_GET:
		return PushdownGet(move(op));
	default:
		return FinishPushdown(move(op));
	}
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownJoin(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op->type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
	       op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
	auto &join = (LogicalJoin &)*op;
	unordered_set<idx_t> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op->children[1], right_bindings);

	switch (join.join_type) {
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
void FilterPushdown::PushFilters() {
	for (auto &f : filters) {
		auto result = combiner.AddFilter(move(f->filter));
		D_ASSERT(result == FilterResult::SUCCESS);
		(void)result;
	}
	filters.clear();
}
FilterResult FilterPushdown::AddFilter(unique_ptr<Expression> expr) {
	PushFilters();
	// split up the filters by AND predicate
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(move(expr));
	LogicalFilter::SplitPredicates(expressions);
	// push the filters into the combiner
	for (auto &expr : expressions) {
		if (combiner.AddFilter(move(expr)) == FilterResult::UNSATISFIABLE) {
			return FilterResult::UNSATISFIABLE;
		}
	}
	return FilterResult::SUCCESS;
}

void FilterPushdown::GenerateFilters() {
	if (filters.size() > 0) {
		D_ASSERT(!combiner.HasFilters());
		return;
	}
	combiner.GenerateFilters([&](unique_ptr<Expression> filter) {
		auto f = make_unique<Filter>();
		f->filter = move(filter);
		f->ExtractBindings();
		filters.push_back(move(f));
	});
}

static Expression *GetColumnRefExpression(Expression &expr) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		return &expr;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { return GetColumnRefExpression(child); });
	return &expr;
}

static bool GenerateBinding(LogicalProjection &proj, BoundColumnRefExpression &colref, ColumnBinding &binding) {
	// assert(colref.binding.table_index != proj.table_index);
	D_ASSERT(colref.depth == 0);
	int column_index = -1;
	// find the corresponding column index in the projection
	for (idx_t proj_idx = 0; proj_idx < proj.expressions.size(); proj_idx++) {
		auto proj_colref = GetColumnRefExpression(*proj.expressions[proj_idx]);
		if (proj_colref->type == ExpressionType::BOUND_COLUMN_REF) {
			// auto proj_colref = (BoundColumnRefExpression *)proj.expressions[proj_idx].get();
			if (colref.Equals(proj_colref)) {
				column_index = proj_idx;
				break;
			}
		}
	}
	// Case the filter column is not projected, returns false
	if (column_index == -1) {
		return false;
	}
	binding.table_index = proj.table_index;
	binding.column_index = column_index;
	return true;
}

static unique_ptr<Expression> ReplaceProjectionBindingsPullup(LogicalProjection &proj, unique_ptr<Expression> expr) {
	// we do not use ExpressionIterator here because we need to check if the filtered column is being projected,
	// otherwise we should avoid the filter to be pulled up by returning nullptr
	if (expr->expression_class == ExpressionClass::BOUND_COMPARISON) {
		auto &comp_expr = (BoundComparisonExpression &)*expr;
		if (comp_expr.left->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &colref = (BoundColumnRefExpression &)*comp_expr.left;
			ColumnBinding binding;
			if (GenerateBinding(proj, colref, binding) == false) {
				// the filtered column is not projected, this filter doesn't need to be pulled up
				return nullptr;
			}
			comp_expr.left =
			    make_unique<BoundColumnRefExpression>(colref.alias, colref.return_type, binding, colref.depth);
		}
		if (comp_expr.right->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &colref = (BoundColumnRefExpression &)*comp_expr.right;
			ColumnBinding binding;
			if (GenerateBinding(proj, colref, binding) == false) {
				// the filtered column is not projected, this filter doesn't need to be pulled up
				return nullptr;
			}
			comp_expr.right =
			    make_unique<BoundColumnRefExpression>(colref.alias, colref.return_type, binding, colref.depth);
		}
	}
	return expr;
}

void FilterPushdown::GenerateFiltersPullup(LogicalOperator &op) {
	combiner_pullup.GenerateFilters([&](unique_ptr<Expression> filter) {
		auto f = make_unique<Filter>();
		f->filter = move(filter);
		f->ExtractBindings();
		if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto new_filter = ReplaceProjectionBindingsPullup((LogicalProjection &)op, move(f->filter));
			if (new_filter != nullptr) {
				f->filter = move(new_filter);
				filters.push_back(move(f));
			}
		} else if (op.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
			filters.push_back(move(f));
		}
	});
}

unique_ptr<LogicalOperator> FilterPushdown::FinishPushdown(unique_ptr<LogicalOperator> op) {
	// unhandled type, first perform filter pushdown in its children
	for (idx_t i = 0; i < op->children.size(); i++) {
		FilterPushdown pushdown(optimizer);
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

void FilterPushdown::Filter::ExtractBindings() {
	bindings.clear();
	LogicalJoin::GetExpressionBindings(*filter, bindings);
}

} // namespace duckdb
