#include "optimizer/rule/case_simplification.hpp"
#include "execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

class CaseConstantExpressionMatcher : public ExpressionMatcher {
public:
	CaseConstantExpressionMatcher() : ExpressionMatcher(ExpressionClass::INVALID) {
	}

	bool Match(Expression *expr, vector<Expression *> &bindings) override {
		// we match on ANY expression that is a scalar expression
		if (!expr->IsScalar()) {
			return false;
		}
		// ...except if it is an Aggregate or Window function
		if (expr->IsAggregate() || expr->IsWindow()) {
			return false;
		}
		// also, if an expression contains a parameter for a prepared statement anywhere, we do not match
		if (expr->HasParameter()) {
			return false;
		}
		bindings.push_back(expr);
		return true;
	}
};

CaseSimplificationRule::CaseSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a CaseExpression that has a ConstantExpression as a check
	auto op = make_unique<CaseExpressionMatcher>();
	op->check = make_unique<CaseConstantExpressionMatcher>();
	root = move(op);
}

unique_ptr<Expression> CaseSimplificationRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                           bool &changes_made) {
	auto root = (CaseExpression *)bindings[0];
	auto constant_expr = bindings[1];
	// the constant_expr is a scalar expression that we have to fold
	assert(constant_expr->IsScalar());

	// use an ExpressionExecutor to execute the expression
	auto constant_value = ExpressionExecutor::EvaluateScalar(*constant_expr);

	// fold based on the constant condition
	auto condition = constant_value.CastAs(TypeId::BOOLEAN);
	if (condition.is_null || !condition.value_.boolean) {
		return move(root->result_if_false);
	} else {
		return move(root->result_if_true);
	}
}
