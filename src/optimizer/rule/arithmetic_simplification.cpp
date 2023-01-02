#include "duckdb/optimizer/rule/arithmetic_simplification.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"

namespace duckdb {

ArithmeticSimplificationRule::ArithmeticSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on an OperatorExpression that has a ConstantExpression as child
	auto op = make_unique<FunctionExpressionMatcher>();
	op->matchers.push_back(make_unique<ConstantExpressionMatcher>());
	op->matchers.push_back(make_unique<ExpressionMatcher>());
	op->policy = SetMatcher::Policy::SOME;
	// we only match on simple arithmetic expressions (+, -, *, /)
	op->function = make_unique<ManyFunctionMatcher>(unordered_set<string> {"+", "-", "*", "/"});
	// and only with numeric results
	op->type = make_unique<IntegerTypeMatcher>();
	op->matchers[0]->type = make_unique<IntegerTypeMatcher>();
	op->matchers[1]->type = make_unique<IntegerTypeMatcher>();
	root = std::move(op);
}

unique_ptr<Expression> ArithmeticSimplificationRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                           bool &changes_made, bool is_root) {
	auto root = (BoundFunctionExpression *)bindings[0];
	auto constant = (BoundConstantExpression *)bindings[1];
	int constant_child = root->children[0].get() == constant ? 0 : 1;
	D_ASSERT(root->children.size() == 2);
	(void)root;
	// any arithmetic operator involving NULL is always NULL
	if (constant->value.IsNull()) {
		return make_unique<BoundConstantExpression>(Value(root->return_type));
	}
	auto &func_name = root->function.name;
	if (func_name == "+") {
		if (constant->value == 0) {
			// addition with 0
			// we can remove the entire operator and replace it with the non-constant child
			return std::move(root->children[1 - constant_child]);
		}
	} else if (func_name == "-") {
		if (constant_child == 1 && constant->value == 0) {
			// subtraction by 0
			// we can remove the entire operator and replace it with the non-constant child
			return std::move(root->children[1 - constant_child]);
		}
	} else if (func_name == "*") {
		if (constant->value == 1) {
			// multiply with 1, replace with non-constant child
			return std::move(root->children[1 - constant_child]);
		} else if (constant->value == 0) {
			// multiply by zero: replace with constant or null
			return ExpressionRewriter::ConstantOrNull(std::move(root->children[1 - constant_child]),
			                                          Value::Numeric(root->return_type, 0));
		}
	} else {
		D_ASSERT(func_name == "/");
		if (constant_child == 1) {
			if (constant->value == 1) {
				// divide by 1, replace with non-constant child
				return std::move(root->children[1 - constant_child]);
			} else if (constant->value == 0) {
				// divide by 0, replace with NULL
				return make_unique<BoundConstantExpression>(Value(root->return_type));
			}
		}
	}
	return nullptr;
}
} // namespace duckdb
