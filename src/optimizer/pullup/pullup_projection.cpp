#include "duckdb/optimizer/filter_pullup.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

static bool CanRewrite(const Expression &expr, const expression_map_t<ProjectionIndex> &projections,
                       bool match_computed) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		return projections.find(expr) != projections.end();
	}
	if (match_computed && projections.find(expr) != projections.end()) {
		return true;
	}
	bool can_rewrite = true;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		can_rewrite = can_rewrite && CanRewrite(child, projections, match_computed);
	});
	return can_rewrite;
}

static unique_ptr<Expression> RewritePredicate(unique_ptr<Expression> expr, LogicalProjection &proj,
                                               expression_map_t<ProjectionIndex> &projections, bool match_computed) {
	return ExpressionIterator::PreorderReplace(std::move(expr), [&](const Expression &child) -> unique_ptr<Expression> {
		if (!match_computed && child.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			return nullptr;
		}
		auto entry = projections.find(child);
		if (entry != projections.end()) {
			if (child.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
				return child.Cast<BoundColumnRefExpression>().WithBinding(
				    ColumnBinding(proj.table_index, entry->second));
			}
			return make_uniq<BoundColumnRefExpression>(child.GetAlias(), child.GetReturnType(),
			                                           ColumnBinding(proj.table_index, entry->second));
		}
		if (child.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			return nullptr;
		}
		D_ASSERT(!match_computed);
		auto column_index = ColumnBinding::PushExpression(proj.expressions, child.Copy());
		projections.emplace(*proj.expressions.back(), column_index);
		return child.Cast<BoundColumnRefExpression>().WithBinding(ColumnBinding(proj.table_index, column_index));
	});
}

unique_ptr<LogicalOperator> FilterPullup::PullupProjection(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_PROJECTION);
	auto &proj = op->Cast<LogicalProjection>();
	// Pulling filters above a projection can evaluate its expressions on additional rows.
	for (auto &expr : proj.expressions) {
		if (expr->CanThrow() || expr->IsVolatile()) {
			return FinishPullup(std::move(op));
		}
	}

	op->children[0] = Rewrite(std::move(op->children[0]));
	if (filters_expr_pullup.empty()) {
		return op;
	}
	for (auto &expr : filters_expr_pullup) {
		if (expr->CanThrow() || expr->IsVolatile()) {
			// Keep fallible or volatile predicates and their guards below the projection.
			proj.children[0] = GeneratePullupFilter(std::move(proj.children[0]), filters_expr_pullup);
			return op;
		}
	}

	expression_map_t<ProjectionIndex> projections;
	for (idx_t i = 0; i < proj.expressions.size(); i++) {
		// Keep constants in predicates available for constant-filter inference.
		if (!proj.expressions[i]->IsFoldable() && FilterCombiner::CanMatchExpression(*proj.expressions[i])) {
			projections.emplace(*proj.expressions[i], ProjectionIndex(i));
		}
	}
	bool can_move = can_add_column;
	if (!can_move) {
		can_move = true;
		for (auto &expr : filters_expr_pullup) {
			can_move = can_move && CanRewrite(*expr, projections, false);
		}
	}
	if (can_move) {
		// Preserve the original predicate shape and its visibility at enclosing projections.
		for (auto &expr : filters_expr_pullup) {
			expr = RewritePredicate(std::move(expr), proj, projections, false);
		}
		return op;
	}

	vector<unique_ptr<Expression>> exported;
	auto export_filter = [&](const Expression &expr) {
		if (!FilterCombiner::CanCopyFilter(expr)) {
			return;
		}
		auto compared = FilterCombiner::GetComparedExpression(expr);
		auto &operand = compared ? *compared : expr;
		const bool match_computed = !CanRewrite(operand, projections, false);
		if (!match_computed || CanRewrite(operand, projections, true)) {
			auto copy = expr.Copy();
			bool rewrite_children = compared;
			if (!rewrite_children && BoundComparisonExpression::IsComparison(expr)) {
				rewrite_children = true;
				ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
					rewrite_children &= CanRewrite(child, projections, match_computed);
				});
			}
			// Keep the comparison available for inference, even when it is also a Boolean output.
			if (rewrite_children) {
				ExpressionIterator::EnumerateChildren(*copy, [&](unique_ptr<Expression> &child) {
					child = RewritePredicate(std::move(child), proj, projections, match_computed);
				});
			} else {
				copy = RewritePredicate(std::move(copy), proj, projections, match_computed);
			}
			exported.push_back(std::move(copy));
		}
	};
	for (auto &expr : filters_expr_pullup) {
		export_filter(*expr);
	}
	if (context) {
		FilterCombiner::GenerateProjectionFilters(
		    *context, filters_expr_pullup, [&](const Expression &key) { return CanRewrite(key, projections, true); },
		    [&](unique_ptr<Expression> derived) { export_filter(*derived); });
	}
	// Retain the full batch: its predicates can guard fallible operators above this projection.
	proj.children[0] = GeneratePullupFilter(std::move(proj.children[0]), filters_expr_pullup);
	filters_expr_pullup = std::move(exported);
	return op;
}

} // namespace duckdb
