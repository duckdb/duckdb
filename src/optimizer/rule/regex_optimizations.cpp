#include "duckdb/optimizer/rule/regex_optimizations.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

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

static bool RegexpIsDotMatch(duckdb_re2::Regexp *regexp) {
	return (regexp->nsub() == 0 && regexp->op() == duckdb_re2::kRegexpCharClass && regexp->ToString() == "[^\\n]");
}

static LikeString LikeMatchExists(duckdb_re2::RE2 &pattern) {
	LikeString ret = LikeString();
	auto parent_op = pattern.Regexp()->op();
	auto num_subs = pattern.Regexp()->nsub();
	auto subs = pattern.Regexp()->sub();
	auto cur_sub_index = 0;
	while (cur_sub_index < num_subs) {
		switch (subs[cur_sub_index]->op()) {
		case duckdb_re2::kRegexpStar:
			if (subs[cur_sub_index]->nsub() == 1 && RegexpIsDotMatch(subs[cur_sub_index]->sub()[0])) {
				ret.like_string += "%";
				break;
			}
			ret.exists = false;
			return ret;
		case duckdb_re2::kRegexpLiteralString:
		case duckdb_re2::kRegexpLiteral:
			// means you repeat a literal. Like cannot process this.
			if (parent_op == duckdb_re2::kRegexpStar) {
				ret.exists = false;
				return ret;
			}
			if (cur_sub_index == 0) {
				ret.like_string += "%";
			}
			ret.like_string += subs[cur_sub_index]->ToString();
			if (cur_sub_index + 1 == num_subs) {
				ret.like_string += "%";
			}
			break;
		case duckdb_re2::kRegexpCharClass: {
			if (RegexpIsDotMatch(subs[cur_sub_index])) {
				if (parent_op == duckdb_re2::kRegexpStar) {
					ret.like_string += "%";
				} else {
					ret.like_string += "_";
				}
				break;
			}
			// could be a concatenation of character sets or otherwise. return no SQL like match
			ret.exists = false;
			return ret;
		} // for some reason this is never hit
		case duckdb_re2::kRegexpAnyChar:
			ret.like_string += "_";
			break;
		case duckdb_re2::kRegexpEndText: {
			if (cur_sub_index + 1 < num_subs) {
				// there is still more regexp, but the end of the string had to be matched
				// should be an invalid regexp
				ret.exists = false;
				return ret;
			}
			break;
		}
		case duckdb_re2::kRegexpQuest: {
			auto last_ind = ret.like_string.size() - 1;
			if (ret.like_string.at(last_ind) == '%') {
				break;
			}
		}
		case duckdb_re2::kRegexpBeginText: {
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
	LikeString like_string;
	if (pattern.Regexp()->op() == duckdb_re2::kRegexpCharClass) {
		// TODO: This is probably a regex match like regexp_matches(s, '[AS]')
		// you can go through the runes in the re2 library and iteratively create a conjuction expression
		// of contains(s, 'a') or contains(s, 's') to optimize the regex out.
		return nullptr;
	}
	// check for a like string. If we can convert it to a like string, the like string
	// optimizer will further optimize suffix and prefix things.
	like_string = LikeMatchExists(pattern);
	if (!like_string.exists) {
		return nullptr;
	}
	auto like_expression =
	    make_uniq<BoundFunctionExpression>(root.return_type, LikeFun::GetFunction(), std::move(root.children), nullptr);
	auto parameter = make_uniq<BoundConstantExpression>(Value(std::move(like_string.like_string)));
	like_expression->children[1] = std::move(parameter);
	return std::move(like_expression);
}

} // namespace duckdb
