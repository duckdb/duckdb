#include "duckdb/optimizer/rule/like_optimizations.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"

namespace duckdb {

LikeOptimizationRule::LikeOptimizationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a FunctionExpression that has a foldable ConstantExpression
	auto func = make_uniq<FunctionExpressionMatcher>();
	func->matchers.push_back(make_uniq<ExpressionMatcher>());
	func->matchers.push_back(make_uniq<ConstantExpressionMatcher>());
	func->policy = SetMatcher::Policy::ORDERED;
	// we match on LIKE ("~~") and NOT LIKE ("!~~")
	func->function = make_uniq<ManyFunctionMatcher>(unordered_set<string> {"!~~", "~~"});
	root = std::move(func);
}

static bool PatternIsConstant(const string &pattern) {
	for (idx_t i = 0; i < pattern.size(); i++) {
		if (pattern[i] == '%' || pattern[i] == '_') {
			return false;
		}
	}
	return true;
}

static bool PatternIsPrefix(const string &pattern) {
	idx_t i;
	for (i = pattern.size(); i > 0; i--) {
		if (pattern[i - 1] != '%') {
			break;
		}
	}
	if (i == pattern.size()) {
		// no trailing %
		// cannot be a prefix
		return false;
	}
	// continue to look in the string
	// if there is a % or _ in the string (besides at the very end) this is not a prefix match
	for (; i > 0; i--) {
		if (pattern[i - 1] == '%' || pattern[i - 1] == '_') {
			return false;
		}
	}
	return true;
}

static bool PatternIsSuffix(const string &pattern) {
	idx_t i;
	for (i = 0; i < pattern.size(); i++) {
		if (pattern[i] != '%') {
			break;
		}
	}
	if (i == 0) {
		// no leading %
		// cannot be a suffix
		return false;
	}
	// continue to look in the string
	// if there is a % or _ in the string (besides at the beginning) this is not a suffix match
	for (; i < pattern.size(); i++) {
		if (pattern[i] == '%' || pattern[i] == '_') {
			return false;
		}
	}
	return true;
}

static bool PatternIsContains(const string &pattern) {
	idx_t start;
	idx_t end;
	for (start = 0; start < pattern.size(); start++) {
		if (pattern[start] != '%') {
			break;
		}
	}
	for (end = pattern.size(); end > 0; end--) {
		if (pattern[end - 1] != '%') {
			break;
		}
	}
	if (start == 0 || end == pattern.size()) {
		// contains requires both a leading AND a trailing %
		return false;
	}
	// check if there are any other special characters in the string
	// if there is a % or _ in the string (besides at the beginning/end) this is not a contains match
	for (idx_t i = start; i < end; i++) {
		if (pattern[i] == '%' || pattern[i] == '_') {
			return false;
		}
	}
	return true;
}

unique_ptr<Expression> LikeOptimizationRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                   bool &changes_made, bool is_root) {
	auto &root = bindings[0].get().Cast<BoundFunctionExpression>();
	auto &constant_expr = bindings[2].get().Cast<BoundConstantExpression>();
	D_ASSERT(root.children.size() == 2);

	if (constant_expr.value.IsNull()) {
		return make_uniq<BoundConstantExpression>(Value(root.return_type));
	}

	// the constant_expr is a scalar expression that we have to fold
	if (!constant_expr.IsFoldable()) {
		return nullptr;
	}

	auto constant_value = ExpressionExecutor::EvaluateScalar(GetContext(), constant_expr);
	D_ASSERT(constant_value.type() == constant_expr.return_type);
	auto &patt_str = StringValue::Get(constant_value);

	bool is_not_like = root.function.name == "!~~";
	if (PatternIsConstant(patt_str)) {
		// Pattern is constant
		return make_uniq<BoundComparisonExpression>(is_not_like ? ExpressionType::COMPARE_NOTEQUAL
		                                                        : ExpressionType::COMPARE_EQUAL,
		                                            std::move(root.children[0]), std::move(root.children[1]));
	} else if (PatternIsPrefix(patt_str)) {
		// Prefix LIKE pattern : [^%_]*[%]+, ignoring underscore
		return ApplyRule(root, PrefixFun::GetFunction(), patt_str, is_not_like);
	} else if (PatternIsSuffix(patt_str)) {
		// Suffix LIKE pattern: [%]+[^%_]*, ignoring underscore
		return ApplyRule(root, SuffixFun::GetFunction(), patt_str, is_not_like);
	} else if (PatternIsContains(patt_str)) {
		// Contains LIKE pattern: [%]+[^%_]*[%]+, ignoring underscore
		return ApplyRule(root, GetStringContains(), patt_str, is_not_like);
	}
	return nullptr;
}

unique_ptr<Expression> LikeOptimizationRule::ApplyRule(BoundFunctionExpression &expr, ScalarFunction function,
                                                       string pattern, bool is_not_like) {
	// replace LIKE by an optimized function
	unique_ptr<Expression> result;
	auto new_function =
	    make_uniq<BoundFunctionExpression>(expr.return_type, std::move(function), std::move(expr.children), nullptr);

	// removing "%" from the pattern
	pattern.erase(std::remove(pattern.begin(), pattern.end(), '%'), pattern.end());

	new_function->children[1] = make_uniq<BoundConstantExpression>(Value(std::move(pattern)));

	result = std::move(new_function);
	if (is_not_like) {
		auto negation = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_NOT, LogicalType::BOOLEAN);
		negation->children.push_back(std::move(result));
		result = std::move(negation);
	}

	return result;
}

} // namespace duckdb
