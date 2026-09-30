#include "duckdb/optimizer/constant_or_null_simplification.hpp"

#include "duckdb/function/scalar/generic_common.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression_nullability.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
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

//! Whether every input that can still turn the result into NULL is provably NOT NULL,
//! according to the injected proof.
static bool ConstantOrNullInputsAreNotNull(BoundFunctionExpression &func, const NotNullProof &proof) {
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

		if (!proof(*children[child_idx])) {
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

//! Nullability proof for join conditions: a condition is evaluated per pair of child rows
//! (before any NULL extension), so the proof must come from the join child that owns the
//! column, not from the join output.
static NotNullProof JoinConditionProof(LogicalOperator &join_op, NotNullExpressionAnalyzer &analyzer) {
	return [&join_op, &analyzer](Expression &input) {
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
		return analyzer.IsNotNull(*join_op.children[side.GetIndex()], input);
	};
}

unique_ptr<Expression> ConstantOrNullSimplification::SimplifyExpression(unique_ptr<Expression> expr,
                                                                        const NotNullProof &proof, bool allow_folding) {
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		child = SimplifyExpression(std::move(child), proof, allow_folding);
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
		if (!ConstantOrNullInputsAreVolatile(func) && ConstantOrNullInputsAreNotNull(func, proof)) {
			return make_uniq<BoundConstantExpression>(Value::BOOLEAN(value.value()));
		}

		return expr;
	}

	if (expr->GetExpressionType() != ExpressionType::OPERATOR_NOT) {
		return expr;
	}

	// Push NOT into constant_or_null without dropping per-row NULL checks.
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

	return ExpressionRewriter::ConstantOrNull(this->context, std::move(children), Value::BOOLEAN(!value.value()));
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
	auto proof = [&filter, &analyzer](Expression &input_expr) {
		return analyzer.IsNotNull(*filter.children[0], input_expr);
	};
	vector<unique_ptr<Expression>> remaining_expressions;
	remaining_expressions.reserve(filter.expressions.size());
	for (auto &expr : filter.expressions) {
		expr = SimplifyExpression(std::move(expr), proof, allow_folding);
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
		any_join.condition = SimplifyExpression(std::move(any_join.condition), JoinConditionProof(*op, analyzer),
		                                        !plan_has_side_effects);
		break;
	}
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		auto &join = op->Cast<LogicalComparisonJoin>();
		NotNullExpressionAnalyzer analyzer(context);
		const bool allow_folding = !plan_has_side_effects;
		auto proof = JoinConditionProof(*op, analyzer);
		for (auto &cond : join.conditions) {
			if (cond.IsComparison()) {
				cond.LeftReference() = SimplifyExpression(std::move(cond.LeftReference()), proof, allow_folding);
				cond.RightReference() = SimplifyExpression(std::move(cond.RightReference()), proof, allow_folding);
			} else {
				cond.JoinExpressionReference() =
				    SimplifyExpression(std::move(cond.JoinExpressionReference()), proof, allow_folding);
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
