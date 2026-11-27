
#include "duckdb/optimizer/rule/constant_folding.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

//! The ConstantFoldingExpressionMatcher matches on any scalar expression (i.e. Expression::IsFoldable is true)
class ConstantFoldingExpressionMatcher : public FoldableConstantMatcher {
public:
	bool Match(Expression &expr, vector<reference<Expression>> &bindings) override {
		// we also do not match on ConstantExpressions, because we cannot fold those any further
		if (expr.GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
			return false;
		}
		return FoldableConstantMatcher::Match(expr, bindings);
	}
};

ConstantFoldingRule::ConstantFoldingRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	auto op = make_uniq<ConstantFoldingExpressionMatcher>();
	root = std::move(op);
}

//! Folds a cast of a constant by running its bound cast directly, as the ExpressionExecutor would
static unique_ptr<Expression> FoldConstantCast(const BoundFunctionExpression &cast_expr, const Value &input) {
	auto &target_type = cast_expr.GetReturnType();
	if (input.IsNull() && cast_expr.Function().GetNullHandling() == FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		return make_uniq<BoundConstantExpression>(Value(target_type));
	}
	Vector source(input, count_t(1));
	Vector result(target_type, 1);
	try {
		BoundCastExpression::ExecuteCast(cast_expr, source, result, 1, nullptr);
	} catch (InternalException &ex) {
		throw;
	} catch (...) {
		// leave the cast in the plan, so that it throws if it is evaluated
		return nullptr;
	}
	auto result_value = result.GetValue(0);
	D_ASSERT(result_value.type().InternalType() == target_type.InternalType());
	return make_uniq<BoundConstantExpression>(result_value.WithType(target_type));
}

unique_ptr<Expression> ConstantFoldingRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                  bool &changes_made, bool is_root) {
	auto &root = bindings[0].get();
	// the root is a scalar expression that we have to fold
	D_ASSERT(root.IsFoldable() && root.GetExpressionType() != ExpressionType::VALUE_CONSTANT);

	if (BoundCastExpression::IsCast(root)) {
		auto &cast_expr = root.Cast<BoundFunctionExpression>();
		auto &child = BoundCastExpression::Child(cast_expr);
		if (child.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
		    !BoundCastExpression::GetBoundCast(cast_expr).HasInitLocalState()) {
			return FoldConstantCast(cast_expr, child.Cast<BoundConstantExpression>().GetValue());
		}
	}
	// use an ExpressionExecutor to execute the expression
	Value result_value;
	if (!ExpressionExecutor::TryEvaluateScalar(GetContext(), root, result_value)) {
		return nullptr;
	}
	D_ASSERT(result_value.type().InternalType() == root.GetReturnType().InternalType());
	// now get the value from the result vector and insert it back into the plan as a constant expression
	return make_uniq<BoundConstantExpression>(result_value);
}

} // namespace duckdb
