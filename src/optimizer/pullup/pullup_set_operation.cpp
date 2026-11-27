#include "duckdb/optimizer/filter_pullup.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

static unique_ptr<Expression> BindSetConstraint(unique_ptr<Expression> filter, LogicalSetOperation &setop,
                                                const vector<ColumnBinding> &bindings) {
	return ExpressionIterator::PreorderReplace<BoundColumnRefExpression>(
	    std::move(filter), [&](const BoundColumnRefExpression &colref) {
		    D_ASSERT(colref.Depth() == 0);
		    for (idx_t i = 0; i < bindings.size(); i++) {
			    if (bindings[i] == colref.Binding()) {
				    return colref.WithBinding(ColumnBinding(setop.table_index, ProjectionIndex(i)));
			    }
		    }
		    throw InternalException("Filter of a set operation refers to a column that is not one of its child");
	    });
}

unique_ptr<LogicalOperator> FilterPullup::PullupSetOperation(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_INTERSECT || op->type == LogicalOperatorType::LOGICAL_EXCEPT);
	if (!context) {
		return FinishPullup(std::move(op));
	}
	auto &setop = op->Cast<LogicalSetOperation>();
	FilterPullup left_pullup(context, true, false);
	FilterPullup right_pullup(context, op->type == LogicalOperatorType::LOGICAL_INTERSECT, false);
	op->children[0] = left_pullup.Rewrite(std::move(op->children[0]));
	op->children[1] = right_pullup.Rewrite(std::move(op->children[1]));
	auto comparisons = setop.GetComparisons(*context);
	vector<reference<const Expression>> left_keys, right_keys;
	for (auto &comparison : comparisons) {
		left_keys.push_back(comparison.GetLHS());
		right_keys.push_back(comparison.GetRHS());
	}
	vector<unique_ptr<Expression>> constraints;
	for (auto &filter : left_pullup.filters_expr_pullup) {
		if (FilterCombiner::IsKeyConstraint(*filter, left_keys)) {
			constraints.push_back(filter->Copy());
		}
	}
	for (auto &filter : right_pullup.filters_expr_pullup) {
		if (!FilterCombiner::IsKeyConstraint(*filter, right_keys)) {
			continue;
		}
		auto copy =
		    ExpressionIterator::PreorderReplace(filter->Copy(), [&](const Expression &expr) -> unique_ptr<Expression> {
			    for (auto &comparison : comparisons) {
				    if (comparison.GetRHS().Equals(expr)) {
					    return comparison.GetLHS().Copy();
				    }
			    }
			    return nullptr;
		    });
		if (FilterCombiner::IsKeyConstraint(*copy, left_keys)) {
			constraints.push_back(std::move(copy));
		}
	}
	op->children[0] = GeneratePullupFilter(std::move(op->children[0]), left_pullup.filters_expr_pullup);
	op->children[1] = GeneratePullupFilter(std::move(op->children[1]), right_pullup.filters_expr_pullup);
	auto bindings = op->children[0]->GetColumnBindings();
	for (auto &filter : constraints) {
		filter = BindSetConstraint(std::move(filter), setop, bindings);
	}
	return ReturnConstraints(std::move(op), std::move(constraints));
}

unique_ptr<LogicalOperator> FilterPullup::PullupUnion(unique_ptr<LogicalOperator> op) {
	if (!can_pullup) {
		return FinishPullup(std::move(op));
	}
	auto &setop = op->Cast<LogicalSetOperation>();
	vector<unique_ptr<Expression>> common;
	for (idx_t child_idx = 0; child_idx < op->children.size(); child_idx++) {
		auto &child = op->children[child_idx];
		FilterPullup child_pullup(context, true, false);
		child = child_pullup.Rewrite(std::move(child));
		auto bindings = child->GetColumnBindings();
		child->ResolveOperatorTypes();
		vector<unique_ptr<Expression>> key_expressions;
		vector<reference<const Expression>> keys;
		for (idx_t i = 0; i < bindings.size(); i++) {
			auto key = make_uniq<BoundColumnRefExpression>(child->types[i], bindings[i]);
			key_expressions.push_back(std::move(key));
			if (!setop.setop_all && context) {
				ExpressionBinder::PushCollation(*context, key_expressions.back(), child->types[i]);
			}
			keys.push_back(*key_expressions.back());
		}
		vector<unique_ptr<Expression>> constraints;
		// The binder has already coerced each branch to the UNION's output types.
		for (auto &filter : child_pullup.filters_expr_pullup) {
			if (FilterCombiner::IsKeyConstraint(*filter, keys) && FilterCombiner::CanMatchExpression(*filter)) {
				constraints.push_back(BindSetConstraint(filter->Copy(), setop, bindings));
			}
		}
		child = GeneratePullupFilter(std::move(child), child_pullup.filters_expr_pullup);
		if (child_idx == 0) {
			FilterCombiner::DeduplicateFilters(constraints);
			common = std::move(constraints);
			continue;
		}
		expression_set_t branch_constraints;
		for (auto &filter : constraints) {
			branch_constraints.insert(*filter);
		}
		for (idx_t i = 0; i < common.size(); i++) {
			if (!branch_constraints.count(*common[i])) {
				common.erase_at(i--);
			}
		}
	}
	return ReturnConstraints(std::move(op), std::move(common));
}

} // namespace duckdb
