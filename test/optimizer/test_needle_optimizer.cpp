#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/optimizer/rule/empty_needle_removal.hpp"
#include "duckdb/optimizer/rule/constant_folding.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "expression_helper.hpp"

using namespace duckdb;
using namespace std;

static void require_case(ExpressionHelper &helper, string input) {
	auto root = helper.ParseExpression(input);
	auto result = helper.ApplyExpressionRule(move(root));
	REQUIRE(result->type == ExpressionType::CASE_EXPR);
}

static void require_constant(ExpressionHelper &helper, string input, Value constant) {
	auto root = helper.ParseExpression(input);
	auto result = helper.ApplyExpressionRule(move(root));
	REQUIRE(result->IsFoldable());
	auto result_value = ExpressionExecutor::EvaluateScalar(*result);
	REQUIRE(((constant.is_null && result_value.is_null) || constant == result_value));
}

TEST_CASE("Test Empty Needle Optimization Rules", "[needle-optimizer]") {
	ExpressionHelper helper;

	REQUIRE(helper.AddColumns("S VARCHAR").empty());

	helper.AddRule<EmptyNeedleRemovalRule>();
	helper.AddRule<ConstantFoldingRule>();

	string input, expected_output;

	require_case(helper, "PREFIX(S, '')");
	require_case(helper, "CONTAINS(S, '')");
	require_case(helper, "SUFFIX(S, '')");

	require_constant(helper, "PREFIX(S, NULL)", Value());
	require_constant(helper, "PREFIX('', NULL)", Value());
	require_constant(helper, "PREFIX(NULL, NULL)", Value());
	require_constant(helper, "PREFIX(NULL, '')", Value());

	require_constant(helper, "PREFIX('asdf', '')", Value::BOOLEAN(true));
	require_constant(helper, "PREFIX('', '')", Value::BOOLEAN(true));

	require_constant(helper, "CONTAINS(S, NULL)", Value());
	require_constant(helper, "CONTAINS('', NULL)", Value());
	require_constant(helper, "CONTAINS(NULL, NULL)", Value());
	require_constant(helper, "CONTAINS(NULL, '')", Value());

	require_constant(helper, "CONTAINS('asdf', '')", Value::BOOLEAN(true));
	require_constant(helper, "CONTAINS('', '')", Value::BOOLEAN(true));

	require_constant(helper, "SUFFIX(S, NULL)", Value());
	require_constant(helper, "SUFFIX('', NULL)", Value());
	require_constant(helper, "SUFFIX(NULL, NULL)", Value());
	require_constant(helper, "SUFFIX(NULL, '')", Value());

	require_constant(helper, "SUFFIX('asdf', '')", Value::BOOLEAN(true));
	require_constant(helper, "SUFFIX('', '')", Value::BOOLEAN(true));

	// REQUIRE_FAIL ----------------
	input = "PREFIX(S, ' ')";
	REQUIRE(helper.VerifyRewrite(input, input));

	input = "PREFIX(S, 'a')";
	REQUIRE(helper.VerifyRewrite(input, input));

	input = "CONTAINS(S, ' ')";
	REQUIRE(helper.VerifyRewrite(input, input));

	input = "CONTAINS(S, 'a')";
	REQUIRE(helper.VerifyRewrite(input, input));

	input = "SUFFIX(S, ' ')";
	REQUIRE(helper.VerifyRewrite(input, input));

	input = "SUFFIX(S, 'a')";
	REQUIRE(helper.VerifyRewrite(input, input));
}
