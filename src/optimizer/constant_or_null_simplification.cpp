#include "duckdb/optimizer/constant_or_null_simplification.hpp"

#include "duckdb/function/scalar/generic_common.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression_nullability.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

ConstantOrNullSimplification::ConstantOrNullSimplification(ClientContext &context_p) : context(context_p) {
}

static optional<bool> GetBooleanConstant(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return optional<bool>();
	}

	auto &constant = expr.Cast<BoundConstantExpression>().GetValue();
	if (constant.IsNull() || constant.type().id() != LogicalTypeId::BOOLEAN) {
		return optional<bool>();
	}

	return BooleanValue::Get(constant);
}

static optional<bool> GetConstantOrNullBoolean(Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION ||
	    expr.GetReturnType().id() != LogicalTypeId::BOOLEAN) {
		return optional<bool>();
	}

	auto &func = expr.Cast<BoundFunctionExpression>();
	if (ConstantOrNull::IsConstantOrNull(func, Value::BOOLEAN(true))) {
		return true;
	}

	if (ConstantOrNull::IsConstantOrNull(func, Value::BOOLEAN(false))) {
		return false;
	}

	return optional<bool>();
}

//! Whether every input that can still turn the result into NULL is provably NOT NULL.
static bool ConstantOrNullInputsAreNotNull(LogicalOperator &input, BoundFunctionExpression &func,
                                           NotNullExpressionAnalyzer &analyzer) {
	auto &children = func.GetChildren();
	D_ASSERT(children.size() >= 2);

	// constant_or_null(c, x...) evaluates to c, except for rows where any input is NULL -
	// those rows yield NULL instead. Folding the call into the plain constant c is therefore
	// only valid when no row can produce a NULL through an input: non-NULL constant inputs
	// are skipped, and every other input must be proven NOT NULL by the analyzer. A NULL
	// constant input, or an input whose nullability cannot be proven, keeps the per-row
	// evaluation intact.
	for (idx_t child_idx = 1; child_idx < children.size(); ++child_idx) {
		if (children[child_idx]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			auto &constant = children[child_idx]->Cast<BoundConstantExpression>().GetValue();
			if (!constant.IsNull()) {
				continue;
			}
		}

		if (!analyzer.IsNotNull(input, *children[child_idx])) {
			return false;
		}
	}

	return true;
}

//! Whether any input of the constant_or_null call is volatile. Folding replaces the call by its
//! constant, which drops the per-row evaluation of the inputs; the NOT-NULL proof in
//! ConstantOrNullInputsAreNotNull can currently only establish column references or non-NULL
//! constants (neither of which is volatile), but the check keeps folding from ever skipping a
//! volatile evaluation if that proof is extended.
static bool ConstantOrNullInputsAreVolatile(BoundFunctionExpression &func) {
	auto &children = func.GetChildren();
	for (idx_t child_idx = 1; child_idx < children.size(); ++child_idx) {
		if (children[child_idx]->IsVolatile()) {
			return true;
		}
	}
	return false;
}

unique_ptr<Expression> ConstantOrNullSimplification::SimplifyExpression(LogicalOperator &input,
                                                                        unique_ptr<Expression> expr,
                                                                        NotNullExpressionAnalyzer &analyzer,
                                                                        bool allow_folding) {
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		child = SimplifyExpression(input, std::move(child), analyzer, allow_folding);
	});

	if (expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		if (!allow_folding) {
			return expr;
		}
		auto value = GetConstantOrNullBoolean(*expr);
		if (!value.has_value()) {
			return expr;
		}

		auto &func = expr->Cast<BoundFunctionExpression>();
		if (!ConstantOrNullInputsAreVolatile(func) && ConstantOrNullInputsAreNotNull(input, func, analyzer)) {
			return make_uniq<BoundConstantExpression>(Value::BOOLEAN(value.value()));
		}

		return expr;
	}

	if (expr->GetExpressionType() != ExpressionType::OPERATOR_NOT) {
		return expr;
	}

	// The rewrites below are shape-only transformations: NOT(constant_or_null(true, x))
	// becomes constant_or_null(false, x) with x still evaluated per row, and NOT over a
	// boolean constant is pure constant folding. Both keep every expression evaluation
	// intact, so they are safe even in plans that carry side effects (e.g. DML).
	auto &not_expr = expr->Cast<BoundOperatorExpression>();
	D_ASSERT(not_expr.GetChildren().size() == 1);

	auto value = GetBooleanConstant(*not_expr.GetChildren()[0]);
	if (value.has_value()) {
		return make_uniq<BoundConstantExpression>(Value::BOOLEAN(!value.value()));
	}

	value = GetConstantOrNullBoolean(*not_expr.GetChildren()[0]);
	if (!value.has_value()) {
		return expr;
	}

	auto &func = not_expr.GetChildren()[0]->Cast<BoundFunctionExpression>();
	auto &func_children = func.GetChildrenMutable();
	D_ASSERT(func_children.size() >= 2);

	vector<unique_ptr<Expression>> children;
	children.reserve(func_children.size());
	children.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(!value.value())));
	for (idx_t child_idx = 1; child_idx < func_children.size(); ++child_idx) {
		children.push_back(std::move(func_children[child_idx]));
	}

	return ExpressionRewriter::ConstantOrNull(std::move(children), Value::BOOLEAN(!value.value()));
}

unique_ptr<LogicalOperator> ConstantOrNullSimplification::OptimizeFilter(unique_ptr<LogicalOperator> op,
                                                                         bool plan_has_side_effects) {
	auto &filter = op->Cast<LogicalFilter>();
	if (filter.children.size() != 1) {
		return op;
	}

	// Folding constant_or_null into a constant drops the evaluation of the folded-away inputs,
	// so it is only allowed when the plan carries no side effects: a DML operator can invalidate
	// the table statistics that NotNullExpressionAnalyzer relies on (e.g. a DML CTE inserts
	// NULLs that the statement's statistics snapshot, taken before execution, does not see).
	// Volatility is checked per folded input in SimplifyExpression instead of per filter: a
	// volatile expression elsewhere in the filter subtree (a sibling conjunct, or an operator
	// below the filter) keeps being evaluated after the fold and must not disable it. The
	// NOT(constant_or_null(...)) shape rewrite is unaffected by all of this.
	const bool allow_folding = !plan_has_side_effects;

	NotNullExpressionAnalyzer analyzer(context);
	vector<unique_ptr<Expression>> remaining_expressions;
	remaining_expressions.reserve(filter.expressions.size());
	for (auto &expr : filter.expressions) {
		expr = SimplifyExpression(*filter.children[0], std::move(expr), analyzer, allow_folding);
		auto value = GetBooleanConstant(*expr);
		if (!value.has_value()) {
			remaining_expressions.push_back(std::move(expr));
		} else if (!value.value()) {
			return make_uniq<LogicalEmptyResult>(std::move(op));
		}
	}

	if (!remaining_expressions.empty()) {
		filter.expressions = std::move(remaining_expressions);
		return op;
	}

	if (filter.projection_map.empty()) {
		return std::move(filter.children[0]);
	}

	remaining_expressions.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
	filter.expressions = std::move(remaining_expressions);
	return op;
}

unique_ptr<LogicalOperator> ConstantOrNullSimplification::Optimize(unique_ptr<LogicalOperator> op) {
	// whether the plan carries side effects is computed once at the root: DML operators
	// inside the plan (including DML CTEs) invalidate the statistics-based nullability
	// analysis, so folding must be disabled for the whole plan
	const bool has_side_effects = op->HasSideEffects();
	return OptimizeInternal(std::move(op), has_side_effects);
}

unique_ptr<LogicalOperator> ConstantOrNullSimplification::OptimizeInternal(unique_ptr<LogicalOperator> op,
                                                                           bool plan_has_side_effects) {
	for (auto &child : op->children) {
		child = OptimizeInternal(std::move(child), plan_has_side_effects);
	}

	if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		return OptimizeFilter(std::move(op), plan_has_side_effects);
	}

	return op;
}

} // namespace duckdb
