#include "duckdb/optimizer/rule/move_constants.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

using namespace duckdb;
using namespace std;

MoveConstantsRule::MoveConstantsRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	auto op = make_unique<ComparisonExpressionMatcher>();
	op->matchers.push_back(make_unique<ConstantExpressionMatcher>());
	op->policy = SetMatcher::Policy::UNORDERED;

	auto arithmetic = make_unique<FunctionExpressionMatcher>();
	// we handle multiplication, addition and subtraction because those are "easy"
	// integer division makes the division case difficult
	// e.g. [x / 2 = 3] means [x = 6 OR x = 7] because of truncation -> no clean rewrite rules
	arithmetic->function = make_unique<ManyFunctionMatcher>(unordered_set<string>{"+", "-", "*"});
	// we match only on integral numeric types
	arithmetic->type = make_unique<IntegerTypeMatcher>();
	arithmetic->matchers.push_back(make_unique<ConstantExpressionMatcher>());
	arithmetic->matchers.push_back(make_unique<ExpressionMatcher>());
	arithmetic->policy = SetMatcher::Policy::SOME;
	op->matchers.push_back(move(arithmetic));
	root = move(op);
}

unique_ptr<Expression> MoveConstantsRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                bool &changes_made) {
	auto comparison = (BoundComparisonExpression *)bindings[0];
	auto outer_constant = (BoundConstantExpression *)bindings[1];
	auto arithmetic = (BoundFunctionExpression *)bindings[2];
	auto inner_constant = (BoundConstantExpression *)bindings[3];

	int arithmetic_child_index = arithmetic->children[0].get() == inner_constant ? 1 : 0;
	auto &op_type = arithmetic->function.name;
	if (op_type == "+") {
		// [x + 1 COMP 10] OR [1 + x COMP 10]
		// order does not matter in addition:
		// simply change right side to 10-1 (outer_constant - inner_constant)
		outer_constant->value = outer_constant->value - inner_constant->value;
	} else if (op_type == "-") {
		// [x - 1 COMP 10] O R [1 - x COMP 10]
		// order matters in subtraction:
		if (arithmetic_child_index == 0) {
			// [x - 1 COMP 10]
			// change right side to 10+1 (outer_constant + inner_constant)
			outer_constant->value = outer_constant->value + inner_constant->value;
		} else {
			// [1 - x COMP 10]
			// change right side to 1-10=-9
			outer_constant->value = inner_constant->value - outer_constant->value;
			// in this case, we should also flip the comparison
			// e.g. if we have [4 - x < 2] then we should have [x > 2]
			comparison->type = FlipComparisionExpression(comparison->type);
		}
	} else {
		assert(op_type == "*");
		// [x * 2 COMP 10] OR [2 * x COMP 10]
		// order does not matter in multiplication:
		// change right side to 10/2 (outer_constant / inner_constant)
		// but ONLY if outer_constant is cleanly divisible by the inner_constant
		if (inner_constant->value == 0) {
			// x * 0, the result is either 0 or NULL
			// thus the final result will be either [TRUE, FALSE] or [NULL], depending
			// on if 0 matches the comparison criteria with the RHS
			// for now we don't fold, but we can fold to "ConstantOrNull"
			return nullptr;
		}
		if (ValueOperations::Modulo(outer_constant->value, inner_constant->value) != 0) {
			// not cleanly divisible, the result will be either FALSE or NULL
			// for now, we don't do anything
			return nullptr;
		}
		if (inner_constant->value < 0) {
			// multiply by negative value, need to flip expression
			comparison->type = FlipComparisionExpression(comparison->type);
		}
		// else divide the RHS by the LHS
		outer_constant->value = outer_constant->value / inner_constant->value;
	}
	// replace left side with x
	// first extract x from the arithmetic expression
	auto arithmetic_child = move(arithmetic->children[arithmetic_child_index]);
	// then place in the comparison
	if (comparison->left.get() == outer_constant) {
		comparison->right = move(arithmetic_child);
	} else {
		comparison->left = move(arithmetic_child);
	}
	changes_made = true;
	return nullptr;
}
