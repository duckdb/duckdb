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
	bool exists = false;
	bool escaped = false;
	string escaped_character = "\\";
	string like_string = "";
};

static LikeString GetEscapedLikeString(duckdb_re2::Regexp *regexp) {
	D_ASSERT(regexp->op() == duckdb_re2::kRegexpLiteralString || regexp->op() == duckdb_re2::kRegexpLiteral);
	LikeString ret;
	if (regexp->op() == duckdb_re2::kRegexpLiteralString) {
		auto nrunes = (idx_t)regexp->nrunes();
		auto runes = regexp->runes();
		for (idx_t i = 0; i < nrunes; i++) {
			auto chr = toascii(runes[i]);
			// if a character is equal to the escaped character return that there is no escaped like string.
			if (chr == '\\') {
				return ret;
			}
			if (chr == '%' || chr == '_') {
				ret.escaped = true;
				ret.like_string += ret.escaped_character;
			}
			ret.like_string += toascii(runes[i]);
		}
	} else {
		auto rune = regexp->rune();
		auto chr = toascii(rune);
		// if a character is equal to the escaped character return that there is no escaped like string.
		if (chr == '\\') {
			return ret;
		}
		if (chr == '%' || chr == '_') {
			ret.escaped = true;
			ret.like_string += ret.escaped_character;
		}
		ret.like_string += toascii(rune);
	}
	// escape min and max from LIKE wild cards.
	ret.exists = true;
	return ret;
}

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
			return ret;
		case duckdb_re2::kRegexpLiteralString:
		case duckdb_re2::kRegexpLiteral: {
			if (cur_sub_index == 0) {
				ret.like_string += "%";
			}
			LikeString escaped_like_string = GetEscapedLikeString(subs[cur_sub_index]);
			if (!escaped_like_string.exists) {
				return escaped_like_string;
			}
			ret.like_string += escaped_like_string.like_string;
			ret.escaped = escaped_like_string.escaped;
			if (cur_sub_index + 1 == num_subs) {
				ret.like_string += "%";
			}
			break;
		}
		case duckdb_re2::kRegexpEndText:
		case duckdb_re2::kRegexpEmptyMatch:
		case duckdb_re2::kRegexpBeginText: {
			break;
		}
		default:
			// some other regexp op that doesn't have an equivalent to a like string
			// return false;
			return ret;
		}
		cur_sub_index += 1;
	}
	ret.exists = true;
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

	LikeString like_string;
	// check for a like string. If we can convert it to a like string, the like string
	// optimizer will further optimize suffix and prefix things.
	if (pattern.Regexp()->op() == duckdb_re2::kRegexpConcat) {
		like_string = LikeMatchExists(pattern);
	} else if (pattern.Regexp()->op() == duckdb_re2::kRegexpLiteralString ||
	           pattern.Regexp()->op() == duckdb_re2::kRegexpLiteral) {
		LikeString escaped_like_string = GetEscapedLikeString(pattern.Regexp());
		if (!escaped_like_string.exists) {
			return nullptr;
		}
		like_string.like_string = "%" + escaped_like_string.like_string + "%";
		like_string.escaped = escaped_like_string.escaped;
		like_string.exists = true;
	}

	if (!like_string.exists) {
		return nullptr;
	}

	// if regexp had options, remove them the new Like Expression can be matched with.
	if (root.children.size() == 3) {
		root.children.pop_back();
		D_ASSERT(root.children.size() == 2);
	}

	if (like_string.escaped) {
		auto like_escape_expression = make_uniq<BoundFunctionExpression>(root.return_type, LikeEscapeFun::GetLikeEscapeFun(),
		                                                          std::move(root.children), nullptr);
		auto parameter = make_uniq<BoundConstantExpression>(Value(std::move(like_string.like_string)));
		auto escape = make_uniq<BoundConstantExpression>(Value(std::move(like_string.escaped_character)));
		like_escape_expression->children[1] = std::move(parameter);
		like_escape_expression->children.push_back(std::move(escape));
		return std::move(like_escape_expression);
	}
	auto like_expression = make_uniq<BoundFunctionExpression>(root.return_type, LikeFun::GetLikeFunction(),
	                                                          std::move(root.children), nullptr);
	auto parameter = make_uniq<BoundConstantExpression>(Value(std::move(like_string.like_string)));
	like_expression->children[1] = std::move(parameter);
	return std::move(like_expression);
}

} // namespace duckdb
