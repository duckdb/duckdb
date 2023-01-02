#include "duckdb/optimizer/rule/constant_folding.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

//! The ConstantFoldingExpressionMatcher matches on any scalar expression (i.e. Expression::IsFoldable is true)
class ConstantFoldingExpressionMatcher : public FoldableConstantMatcher {
public:
	bool Match(Expression *expr, vector<Expression *> &bindings) override {
		// we also do not match on ConstantExpressions, because we cannot fold those any further
		if (expr->type == ExpressionType::VALUE_CONSTANT) {
			return false;
		}
		return FoldableConstantMatcher::Match(expr, bindings);
	}
};

ConstantFoldingRule::ConstantFoldingRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	auto op = make_unique<ConstantFoldingExpressionMatcher>();
	root = std::move(op);
}

unique_ptr<Expression> ConstantFoldingRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                  bool &changes_made, bool is_root) {
	auto root = bindings[0];
	// the root is a scalar expression that we have to fold
	D_ASSERT(root->IsFoldable() && root->type != ExpressionType::VALUE_CONSTANT);

	// use an ExpressionExecutor to execute the expression
	Value result_value;
	if (!ExpressionExecutor::TryEvaluateScalar(GetContext(), *root, result_value)) {
		return nullptr;
	}
	D_ASSERT(result_value.type().InternalType() == root->return_type.InternalType());
	// now get the value from the result vector and insert it back into the plan as a constant expression
	return make_unique<BoundConstantExpression>(result_value);
}

} // namespace duckdb
