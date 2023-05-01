#include "duckdb/optimizer/rule/regex_optimizations.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/scalar/regexp.hpp"

#include "re2/re2.h"
#include "re2/regexp.h"

namespace duckdb {

RegexOptimizationRule::RegexOptimizationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	auto func = make_uniq<FunctionExpressionMatcher>();
	func->function = make_uniq<SpecificFunctionMatcher>("regexp_matches");
	func->policy = SetMatcher::Policy::PARTIAL_ORDERED;
	func->matchers.push_back(make_uniq<ExpressionMatcher>());
	func->matchers.push_back(make_uniq<ConstantExpressionMatcher>());
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
		case duckdb_re2::kRegexpAnyChar:
			if (cur_sub_index == 0) {
				ret.like_string += "%";
			}
			ret.like_string += "_";
			if (cur_sub_index + 1 == num_subs) {
				ret.like_string += "%";
			}
			break;
		case duckdb_re2::kRegexpStar:
			// .* is a Star operator is a anyChar operator as a child.
			// any other child operator would represent a pattern LIKE cannot match.
			if (subs[cur_sub_index]->nsub() == 1 && subs[cur_sub_index]->sub()[0]->op() == duckdb_re2::kRegexpAnyChar) {
				ret.like_string += "%";
				break;
			}
			ret.exists = false;
			return ret;
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
		case duckdb_re2::kRegexpEndText:
		case duckdb_re2::kRegexpEmptyMatch:
		case duckdb_re2::kRegexpBeginText: {
			break;
		}
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
	D_ASSERT(root.children.size() == 2 || root.children.size() == 3);
	auto regexp_bind_data = root.bind_info.get()->Cast<RegexpMatchesBindData>();
	duckdb_re2::RE2::Options parsed_options = regexp_bind_data.options;

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

	duckdb_re2::RE2 pattern(patt_str, parsed_options);
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
	} else if (pattern.Regexp()->op() != duckdb_re2::kRegexpConcat) {
		return nullptr;
	}
	LikeString like_string;
	// check for a like string. If we can convert it to a like string, the like string
	// optimizer will further optimize suffix and prefix things.
	like_string = LikeMatchExists(pattern);
	if (!like_string.exists) {
		return nullptr;
	}

	// if regexp had options, remove them the new Like Expression can be matched with.
	if (root.children.size() == 3) {
		root.children.pop_back();
		D_ASSERT(root.children.size() == 2);
	}

	auto like_expression = make_uniq<BoundFunctionExpression>(root.return_type, LikeFun::GetLikeFunction(),
	                                                          std::move(root.children), nullptr);
	auto parameter = make_uniq<BoundConstantExpression>(Value(std::move(like_string.like_string)));
	like_expression->children[1] = std::move(parameter);
	return std::move(like_expression);
}

} // namespace duckdb
