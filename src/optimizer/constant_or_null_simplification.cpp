#include "duckdb/optimizer/constant_or_null_simplification.hpp"

#include "duckdb/function/scalar/generic_common.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
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

	// Folding is only valid when the NULL-preserving inputs cannot be NULL.
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

static bool ConstantOrNullInputsAreVolatile(BoundFunctionExpression &func) {
	auto &children = func.GetChildren();
	for (idx_t child_idx = 1; child_idx < children.size(); ++child_idx) {
		if (children[child_idx]->IsVolatile()) {
			return true;
		}
	}
	return false;
}

//! Push NOT into a constant_or_null (or into a plain constant) on a single expression node.
//! Pure rewrite that keeps per-row NULL checks - also valid for side-effecting plans.
static unique_ptr<Expression> ApplyNotPushdown(ClientContext &context, unique_ptr<Expression> expr) {
	if (expr->GetExpressionType() != ExpressionType::OPERATOR_NOT) {
		return expr;
	}

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
	for (idx_t child_idx = 1; child_idx < func_children.size(); ++child_idx) {
		children.push_back(std::move(func_children[child_idx]));
	}

	return ExpressionRewriter::ConstantOrNull(context, std::move(children), Value::BOOLEAN(!value.value()));
}

//! Whether every NULL-sensitive input of the constant_or_null is provably NOT NULL on the output
//! of the join child that binds it. Join conditions are evaluated per pair of child rows (before
//! any NULL extension), so the proof must be taken from the owning child, not from the join output.
static bool JoinConditionInputsAreNotNull(LogicalOperator &join_op, BoundFunctionExpression &func,
                                          NotNullExpressionAnalyzer &analyzer) {
	auto &children = func.GetChildren();
	for (idx_t child_idx = 1; child_idx < children.size(); ++child_idx) {
		auto &input = *children[child_idx];
		if (input.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			auto &constant = input.Cast<BoundConstantExpression>().GetValue();
			if (!constant.IsNull()) {
				continue;
			}
		}
		// Only bare column references can be proven non-NULL
		if (input.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}
		auto &colref = input.Cast<BoundColumnRefExpression>();
		if (colref.Depth() != 0) {
			return false;
		}
		// Locate the join child that binds the column; bail on mixed-side inputs
		optional_idx side;
		for (idx_t side_idx = 0; side_idx < join_op.children.size(); side_idx++) {
			for (auto &binding : join_op.children[side_idx]->GetColumnBindings()) {
				if (binding.table_index != colref.Binding().table_index) {
					continue;
				}
				if (!side.IsValid()) {
					side = side_idx;
				} else if (side.GetIndex() != side_idx) {
					return false;
				}
				break;
			}
		}
		if (!side.IsValid()) {
			return false;
		}
		if (!analyzer.IsNotNull(*join_op.children[side.GetIndex()], input)) {
			return false;
		}
	}
	return true;
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

	return ApplyNotPushdown(context, std::move(expr));
}

unique_ptr<Expression> ConstantOrNullSimplification::SimplifyJoinCondition(LogicalOperator &join_op,
                                                                           NotNullExpressionAnalyzer &analyzer,
                                                                           unique_ptr<Expression> expr,
                                                                           bool allow_folding) {
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		child = SimplifyJoinCondition(join_op, analyzer, std::move(child), allow_folding);
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
		if (!ConstantOrNullInputsAreVolatile(func) && JoinConditionInputsAreNotNull(join_op, func, analyzer)) {
			return make_uniq<BoundConstantExpression>(Value::BOOLEAN(value.value()));
		}

		return expr;
	}

	return ApplyNotPushdown(context, std::move(expr));
}

unique_ptr<LogicalOperator> ConstantOrNullSimplification::OptimizeFilter(unique_ptr<LogicalOperator> op,
                                                                         bool plan_has_side_effects) {
	auto &filter = op->Cast<LogicalFilter>();
	if (filter.children.size() != 1) {
		return op;
	}

	// Folding removes the NULL check, so disable it for plans with side effects.
	// Same-statement DML can add NULLs after statistics-based nullability analysis.
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
	const bool has_side_effects = op->HasSideEffects();
	return OptimizeInternal(std::move(op), has_side_effects);
}

unique_ptr<LogicalOperator> ConstantOrNullSimplification::OptimizeInternal(unique_ptr<LogicalOperator> op,
                                                                           bool plan_has_side_effects) {
	for (auto &child : op->children) {
		child = OptimizeInternal(std::move(child), plan_has_side_effects);
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_FILTER:
		return OptimizeFilter(std::move(op), plan_has_side_effects);
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &any_join = op->Cast<LogicalAnyJoin>();
		NotNullExpressionAnalyzer analyzer(context);
		any_join.condition = SimplifyJoinCondition(*op, analyzer, std::move(any_join.condition), !plan_has_side_effects);
		break;
	}
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		auto &join = op->Cast<LogicalComparisonJoin>();
		NotNullExpressionAnalyzer analyzer(context);
		const bool allow_folding = !plan_has_side_effects;
		for (auto &cond : join.conditions) {
			if (cond.IsComparison()) {
				cond.LeftReference() =
				    SimplifyJoinCondition(*op, analyzer, std::move(cond.LeftReference()), allow_folding);
				cond.RightReference() =
				    SimplifyJoinCondition(*op, analyzer, std::move(cond.RightReference()), allow_folding);
			} else {
				cond.JoinExpressionReference() =
				    SimplifyJoinCondition(*op, analyzer, std::move(cond.JoinExpressionReference()), allow_folding);
			}
		}
		break;
	}
	default:
		break;
	}

	return op;
}

} // namespace duckdb
