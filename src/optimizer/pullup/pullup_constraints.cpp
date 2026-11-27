#include "duckdb/optimizer/filter_pullup.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> FilterPullup::ReturnConstraints(unique_ptr<LogicalOperator> op,
                                                            vector<unique_ptr<Expression>> constraints) {
	FilterCombiner::DeduplicateFilters(constraints);
	if (!can_pullup) {
		return GeneratePullupFilter(std::move(op), constraints);
	}
	for (auto &constraint : constraints) {
		filters_expr_pullup.push_back(std::move(constraint));
	}
	return op;
}

unique_ptr<LogicalOperator> FilterPullup::PullupWindow(unique_ptr<LogicalOperator> op) {
	if (!can_pullup || op->expressions.empty()) {
		return FinishPullup(std::move(op));
	}
	vector<reference<const Expression>> keys;
	for (idx_t i = 0; i < op->expressions.size(); i++) {
		auto &expr = *op->expressions[i];
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_WINDOW || expr.IsVolatile()) {
			return FinishPullup(std::move(op));
		}
		auto &partitions = expr.Cast<BoundWindowExpression>().Partitions();
		if (i == 0) {
			for (auto &partition : partitions) {
				if (FilterCombiner::CanMatchExpression(*partition)) {
					keys.push_back(*partition);
				}
			}
		} else {
			for (idx_t k = 0; k < keys.size(); k++) {
				bool found = false;
				for (auto &partition : partitions) {
					found |= FilterCombiner::CanMatchExpression(*partition) && keys[k].get().Equals(*partition);
				}
				if (!found) {
					keys.erase_at(k--);
				}
			}
		}
	}
	if (keys.empty()) {
		return FinishPullup(std::move(op));
	}
	FilterPullup child_pullup(context, true, false);
	op->children[0] = child_pullup.Rewrite(std::move(op->children[0]));
	vector<unique_ptr<Expression>> constraints;
	for (auto &filter : child_pullup.filters_expr_pullup) {
		if (FilterCombiner::IsKeyConstraint(*filter, keys)) {
			constraints.push_back(filter->Copy());
		}
	}
	// Keep partition membership and the rows evaluated by window arguments and frames unchanged.
	op->children[0] = GeneratePullupFilter(std::move(op->children[0]), child_pullup.filters_expr_pullup);
	return ReturnConstraints(std::move(op), std::move(constraints));
}

static void RewriteGroupConstraint(Expression &filter, TableIndex group_index,
                                   const expression_map_t<ProjectionIndex> &outputs) {
	const auto type = filter.GetExpressionType();
	const bool conjunction = type == ExpressionType::CONJUNCTION_AND || type == ExpressionType::CONJUNCTION_OR ||
	                         type == ExpressionType::OPERATOR_NOT;
	ExpressionIterator::EnumerateChildren(filter, [&](unique_ptr<Expression> &child) {
		if (conjunction) {
			RewriteGroupConstraint(*child, group_index, outputs);
			return;
		}
		child = ExpressionIterator::PreorderReplace(
		    std::move(child), [&](const Expression &expr) -> unique_ptr<Expression> {
			    auto entry = outputs.find(expr);
			    if (entry == outputs.end()) {
				    return nullptr;
			    }
			    return make_uniq<BoundColumnRefExpression>(expr.GetReturnType(),
			                                               ColumnBinding(group_index, entry->second));
		    });
	});
}

unique_ptr<LogicalOperator> FilterPullup::PullupAggregate(unique_ptr<LogicalOperator> op) {
	auto &aggregate = op->Cast<LogicalAggregate>();
	if (!can_pullup || aggregate.groups.empty()) {
		return FinishPullup(std::move(op));
	}
	for (auto &expr : aggregate.expressions) {
		if (expr->IsVolatile()) {
			return FinishPullup(std::move(op));
		}
	}
	vector<reference<const Expression>> keys;
	expression_map_t<ProjectionIndex> outputs;
	for (idx_t i = 0; i < aggregate.groups.size(); i++) {
		auto &group = *aggregate.groups[i];
		if (group.IsFoldable() || group.IsVolatile() || group.CanThrow() ||
		    !FilterCombiner::CanMatchExpression(group)) {
			continue;
		}
		bool common = true;
		for (auto &set : aggregate.grouping_sets) {
			common &= set.count(ProjectionIndex(i)) != 0;
		}
		if (common) {
			keys.push_back(group);
			outputs.emplace(group, ProjectionIndex(i));
		}
	}
	if (keys.empty()) {
		return FinishPullup(std::move(op));
	}
	FilterPullup child_pullup(context, true, false);
	op->children[0] = child_pullup.Rewrite(std::move(op->children[0]));
	vector<unique_ptr<Expression>> constraints;
	for (auto &filter : child_pullup.filters_expr_pullup) {
		if (!FilterCombiner::IsKeyConstraint(*filter, keys)) {
			continue;
		}
		auto copy = filter->Copy();
		// Keep comparison roots and constants available to the filter combiner.
		RewriteGroupConstraint(*copy, aggregate.group_index, outputs);
		constraints.push_back(std::move(copy));
	}
	op->children[0] = GeneratePullupFilter(std::move(op->children[0]), child_pullup.filters_expr_pullup);
	return ReturnConstraints(std::move(op), std::move(constraints));
}

} // namespace duckdb
