#include "duckdb/optimizer/rule/move_constants.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"

namespace duckdb {

MoveConstantsRule::MoveConstantsRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	auto op = make_uniq<ComparisonExpressionMatcher>();
	op->matchers.push_back(make_uniq<ConstantExpressionMatcher>());
	op->policy = SetMatcher::Policy::UNORDERED;

	auto arithmetic = make_uniq<FunctionExpressionMatcher>();
	// we handle multiplication, addition and subtraction because those are "easy"
	// integer division makes the division case difficult
	// e.g. [x / 2 = 3] means [x = 6 OR x = 7] because of truncation -> no clean rewrite rules
	arithmetic->function = make_uniq<ManyFunctionMatcher>(unordered_set<string> {"+", "-", "*"});
	// we match only on integral numeric types
	arithmetic->type = make_uniq<IntegerTypeMatcher>();
	arithmetic->matchers.push_back(make_uniq<ConstantExpressionMatcher>());
	arithmetic->matchers.push_back(make_uniq<ExpressionMatcher>());
	arithmetic->policy = SetMatcher::Policy::SOME;
	op->matchers.push_back(std::move(arithmetic));
	root = std::move(op);
}

unique_ptr<Expression> MoveConstantsRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                bool &changes_made, bool is_root) {
	auto &comparison = bindings[0].get().Cast<BoundComparisonExpression>();
	auto &outer_constant = bindings[1].get().Cast<BoundConstantExpression>();
	auto &arithmetic = bindings[2].get().Cast<BoundFunctionExpression>();
	auto &inner_constant = bindings[3].get().Cast<BoundConstantExpression>();
	if (!TypeIsIntegral(arithmetic.return_type.InternalType())) {
		return nullptr;
	}
	if (inner_constant.value.IsNull() || outer_constant.value.IsNull()) {
		return make_uniq<BoundConstantExpression>(Value(comparison.return_type));
	}
	auto &constant_type = outer_constant.return_type;
	hugeint_t outer_value = IntegralValue::Get(outer_constant.value);
	hugeint_t inner_value = IntegralValue::Get(inner_constant.value);

	idx_t arithmetic_child_index = arithmetic.children[0].get() == &inner_constant ? 1 : 0;
	auto &op_type = arithmetic.function.name;
	if (op_type == "+") {
		// [x + 1 COMP 10] OR [1 + x COMP 10]
		// order does not matter in addition:
		// simply change right side to 10-1 (outer_constant - inner_constant)
		if (!Hugeint::SubtractInPlace(outer_value, inner_value)) {
			return nullptr;
		}
		auto result_value = Value::HUGEINT(outer_value);
		if (!result_value.DefaultTryCastAs(constant_type)) {
			if (comparison.type != ExpressionType::COMPARE_EQUAL) {
				return nullptr;
			}
			// if the cast is not possible then the comparison is not possible
			// for example, if we have x + 5 = 3, where x is an unsigned number, we will get x = -2
			// since this is not possible we can remove the entire branch here
			return ExpressionRewriter::ConstantOrNull(std::move(arithmetic.children[arithmetic_child_index]),
			                                          Value::BOOLEAN(false));
		}
		outer_constant.value = std::move(result_value);
	} else if (op_type == "-") {
		// [x - 1 COMP 10] O R [1 - x COMP 10]
		// order matters in subtraction:
		if (arithmetic_child_index == 0) {
			// [x - 1 COMP 10]
			// change right side to 10+1 (outer_constant + inner_constant)
			if (!Hugeint::AddInPlace(outer_value, inner_value)) {
				return nullptr;
			}
			auto result_value = Value::HUGEINT(outer_value);
			if (!result_value.DefaultTryCastAs(constant_type)) {
				// if the cast is not possible then an equality comparison is not possible
				if (comparison.type != ExpressionType::COMPARE_EQUAL) {
					return nullptr;
				}
				return ExpressionRewriter::ConstantOrNull(std::move(arithmetic.children[arithmetic_child_index]),
				                                          Value::BOOLEAN(false));
			}
			outer_constant.value = std::move(result_value);
		} else {
			// [1 - x COMP 10]
			// change right side to 1-10=-9
			if (!Hugeint::SubtractInPlace(inner_value, outer_value)) {
				return nullptr;
			}
			auto result_value = Value::HUGEINT(inner_value);
			if (!result_value.DefaultTryCastAs(constant_type)) {
				// if the cast is not possible then an equality comparison is not possible
				if (comparison.type != ExpressionType::COMPARE_EQUAL) {
					return nullptr;
				}
				return ExpressionRewriter::ConstantOrNull(std::move(arithmetic.children[arithmetic_child_index]),
				                                          Value::BOOLEAN(false));
			}
			outer_constant.value = std::move(result_value);
			// in this case, we should also flip the comparison
			// e.g. if we have [4 - x < 2] then we should have [x > 2]
			comparison.type = FlipComparisonExpression(comparison.type);
		}
	} else {
		D_ASSERT(op_type == "*");
		// [x * 2 COMP 10] OR [2 * x COMP 10]
		// order does not matter in multiplication:
		// change right side to 10/2 (outer_constant / inner_constant)
		// but ONLY if outer_constant is cleanly divisible by the inner_constant
		if (inner_value == 0) {
			// x * 0, the result is either 0 or NULL
			// we let the arithmetic_simplification rule take care of simplifying this first
			return nullptr;
		}
		if (outer_value % inner_value != 0) {
			// not cleanly divisible
			bool is_equality = comparison.type == ExpressionType::COMPARE_EQUAL;
			bool is_inequality = comparison.type == ExpressionType::COMPARE_NOTEQUAL;
			if (is_equality || is_inequality) {
				// we know the values are not equal
				// the result will be either FALSE or NULL (if COMPARE_EQUAL)
				// or TRUE or NULL (if COMPARE_NOTEQUAL)
				return ExpressionRewriter::ConstantOrNull(std::move(arithmetic.children[arithmetic_child_index]),
				                                          Value::BOOLEAN(is_inequality));
			} else {
				// not cleanly divisible and we are doing > >= < <=, skip the simplification for now
				return nullptr;
			}
		}
		if (inner_value < 0) {
			// multiply by negative value, need to flip expression
			comparison.type = FlipComparisonExpression(comparison.type);
		}
		// else divide the RHS by the LHS
		// we need to do a range check on the cast even though we do a division
		// because e.g. -128 / -1 = 128, which is out of range
		auto result_value = Value::HUGEINT(outer_value / inner_value);
		if (!result_value.DefaultTryCastAs(constant_type)) {
			return ExpressionRewriter::ConstantOrNull(std::move(arithmetic.children[arithmetic_child_index]),
			                                          Value::BOOLEAN(false));
		}
		outer_constant.value = std::move(result_value);
	}
	// replace left side with x
	// first extract x from the arithmetic expression
	auto arithmetic_child = std::move(arithmetic.children[arithmetic_child_index]);
	// then place in the comparison
	if (comparison.left.get() == &outer_constant) {
		comparison.right = std::move(arithmetic_child);
	} else {
		comparison.left = std::move(arithmetic_child);
	}
	changes_made = true;
	return nullptr;
}

} // namespace duckdb
