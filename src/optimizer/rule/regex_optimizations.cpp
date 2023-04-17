#include "duckdb/optimizer/rule/regex_optimizations.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
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

struct LikeString {
	bool exists = true;
	string like_string = "";
};

static LikeString LikeMatchExists(duckdb_re2::RE2 &pattern) {
	LikeString ret = LikeString();
	auto num_subs = pattern.Regexp()->nsub();
	auto subs = pattern.Regexp()->sub();
	auto cur_sub_index = 0;
	while (cur_sub_index < num_subs) {
		switch (subs[cur_sub_index]->op()) {
		case duckdb_re2::kRegexpStar:
			ret.like_string += "%";
			break;
		case duckdb_re2::kRegexpLiteralString:
		case duckdb_re2::kRegexpLiteral:
			if (cur_sub_index == 0) {
				ret.like_string += "%";
			}
			ret.like_string += subs[cur_sub_index]->ToString();
			if (cur_sub_index + 1 == num_subs) {
				ret.like_string += "%";
			}
			break;
		case duckdb_re2::kRegexpCharClass: {
			if (subs[cur_sub_index]->ToString() == "[^\\n]") {
				ret.like_string += "_";
				break;
			}
		}
		case duckdb_re2::kRegexpAnyChar:
			ret.like_string += "_";
			break;
		case duckdb_re2::kRegexpEndText: {
//			ret.like_string += "$";
			if (cur_sub_index + 1 < num_subs) {
				// there is still more regexp, but the end of the string had to be matched
				// should be an invalid regexp
				ret.exists = false;
				return ret;
			}
			break;
		}
		case duckdb_re2::kRegexpQuest: {
			auto last_ind = ret.like_string.size()-1;
			if (ret.like_string.at(last_ind) == '%') {
				break;
			}
		}
		case duckdb_re2::kRegexpBeginText: {
//			ret.like_string += "^";
			if (cur_sub_index != 0) {
				// cur_sub_index should be 0 if we are matching the beginning of the text
				// probably invalid regexp, don't convert
				ret.exists = false;
				return ret;
			}
			break;
		}
		case duckdb_re2::kRegexpEmptyMatch:
			break;
		default:
			// some other regexp op that doesn't have an equivalent to a like string
			// return false;
			ret.exists = false;
			return ret;
		}
		cur_sub_index += 1;
	}
	return ret;
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
	}
		// check for a like string. If we can convert it to a like string, the like string
		// optimizer will further optimize suffix and prefix things.
		auto like_string = LikeMatchExists(pattern);
		if (!like_string.exists) {
			return nullptr;
		}
//		std::cout << like_string.like_string << std::endl;
		auto like_expression = make_uniq<BoundFunctionExpression>(root.return_type, LikeFun::GetFunction(), std::move(root.children), nullptr);
		auto parameter = make_uniq<BoundConstantExpression>(Value(std::move(like_string.like_string)));
//	    like_expression->children[0] = std::move(like_expression->children[1]);
		like_expression->children[1] = std::move(parameter);
		return std::move(like_expression);
//		// check for any prefix or suffix matches
////		string prefix("tmp_prefix");
////		bool fold_case = true;
////		duckdb_re2::RE2 new_regex("");
////		auto regexp = new_regex.Regexp();
////
////		// in case it is suffix and
////		auto conjuction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
////		unique_ptr<BoundFunctionExpression> prefix_expression = nullptr;
////		unique_ptr<BoundFunctionExpression> suffix_expression = nullptr;
////
////		auto num_subs = pattern.Regexp()->nsub();
////		idx_t cur_sub = 0;
////		while (num_subs >= 2 && cur_sub < num_subs) {
////			// check for prefix expression. Here you
////			if (!prefix_expression) {
////				bool prefix_required = pattern.Regexp()->RequiredPrefix(&prefix, &fold_case, &regexp);
////				if (prefix_required) {
////					// means only a prefix was asked for, as the rest of the regex is an empty match.
////					if (regexp->op() == duckdb_re2::kRegexpEmptyMatch) {
////						prefix_expression = make_uniq<BoundFunctionExpression>(
////						    root.return_type, PrefixFun::GetFunction(), std::move(root.children), nullptr);
////						prefix_expression->children[1] = make_uniq<BoundConstantExpression>(Value(std::move(prefix)));
////					} else {
////						// TODO: here you can use the rest of the regexp and make a regex operator
////						auto regex_rest_expression = make_uniq<BoundFunctionExpression>(root.return_type, RegexpFun(), std::move(regexp), nullptr);
////						std::cout << "check here what the rest of the regex is" << std::endl;
////					}
////				}
////			}
////
////			// check for suffix expression.
////			if (pattern.Regexp()->op() == duckdb_re2::kRegexpConcat) {
////				auto num_subs = pattern.Regexp()->nsub();
////				auto subs = pattern.Regexp()->sub();
////				if (num_subs == 2) {
////					auto before_last = subs[num_subs - 2];
////					auto last_sub = subs[num_subs - 1];
////					// check if we match a literal and then the end of the string. If so, the literal is the suffix.
////					if ((before_last->op() == duckdb_re2::kRegexpLiteral ||
////					     before_last->op() == duckdb_re2::kRegexpLiteralString) &&
////					    last_sub->op() == duckdb_re2::kRegexpEndText) {
////						auto suffix_string = before_last->ToString();
////						suffix_expression = make_uniq<BoundFunctionExpression>(root.return_type, SuffixFun::GetFunction(), std::move(root->children), nullptr);
////						suffix_expression->children[1] = make_uniq<BoundConstantExpression>(Value(std::move(suffix_string)));
////					}
////				}
////			}
////
////		}
////		// check for a suffix
////		// check if regexes are concatenated
//
}

} // namespace duckdb
