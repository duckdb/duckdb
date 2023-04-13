#include "duckdb/optimizer/rule/regex_optimizations.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/function/scalar_function.hpp"

#include "re2/re2.h"
#include "re2/regexp.h"
#include "iostream"

namespace duckdb {

RegexOptimizationRule::RegexOptimizationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	auto func = make_uniq<FunctionExpressionMatcher>();
	func->function = make_uniq<SpecificFunctionMatcher>("regexp_matches");
	func->policy = SetMatcher::Policy::ORDERED;
	func->matchers.push_back(make_uniq<ExpressionMatcher>());
	func->matchers.push_back(make_uniq<ConstantExpressionMatcher>());
	root = std::move(func);
}

unique_ptr<Expression> RegexOptimizationRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
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
	auto patt_str = StringValue::Get(constant_value);

	duckdb_re2::RE2 pattern(patt_str);
	if (!pattern.ok()) {
		return nullptr; // this should fail somewhere else
	}

	if (pattern.Regexp()->op() == duckdb_re2::kRegexpLiteralString ||
	    pattern.Regexp()->op() == duckdb_re2::kRegexpLiteral) {

		string min;
		string max;
		pattern.PossibleMatchRange(&min, &max, patt_str.size() + 1);
		if (min != max) {
			return nullptr;
		}
		auto parameter = make_uniq<BoundConstantExpression>(Value(std::move(min)));
		auto contains = make_uniq<BoundFunctionExpression>(root.return_type, ContainsFun::GetFunction(),
		                                                   std::move(root.children), nullptr);
		contains->children[1] = std::move(parameter);

		return std::move(contains);
	} else {
		// check for any prefix or suffix matches
		string prefix("tmp_prefix");
		bool fold_case = true;
		duckdb_re2::RE2 new_regex("");
		auto regexp = new_regex.Regexp();

		bool prefix_required = pattern.Regexp()->RequiredPrefix(&prefix, &fold_case, &regexp);
		if (prefix_required) {
			// means only a prefix was asked for, as the rest of the regex is an empty match.
			if (regexp->op() == duckdb_re2::kRegexpEmptyMatch) {
				auto prefix_expression = make_unique<BoundFunctionExpression>(root->return_type, PrefixFun::GetFunction(), std::move(root->children), nullptr);
				prefix_expression->children[1] = make_unique<BoundConstantExpression>(Value(std::move(prefix)));
				return std::move(prefix_expression);
			}
		}
		// check for a suffix
		// check if regexes are concatenated
		if (pattern.Regexp()->op() == duckdb_re2::kRegexpConcat) {
			auto num_subs = pattern.Regexp()->nsub();
			auto subs = pattern.Regexp()->sub();
			if (num_subs == 2) {
				auto before_last = subs[num_subs - 2];
				auto last_sub = subs[num_subs - 1];
				// check if we match a literal and then the end of the string. If so, the literal is the suffix.
				if ((before_last->op() == duckdb_re2::kRegexpLiteral ||
				     before_last->op() == duckdb_re2::kRegexpLiteralString) &&
				    last_sub->op() == duckdb_re2::kRegexpEndText) {
					auto suffix_string = before_last->ToString();
					auto suffix_expression = make_unique<BoundFunctionExpression>(root->return_type, SuffixFun::GetFunction(), std::move(root->children), nullptr);
					suffix_expression->children[1] = make_unique<BoundConstantExpression>(Value(std::move(suffix_string)));
					return std::move(suffix_expression);
				}
			}
		}
	}

	return nullptr;
}

} // namespace duckdb
