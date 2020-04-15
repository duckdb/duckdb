#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "test_helpers.hpp"
#include "duckdb/optimizer/rule/like_optimizations.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Like Optimization Rules", "[like-optimizer]") {
	ExpressionHelper helper;

	REQUIRE(helper.AddColumns("S VARCHAR").empty());

	helper.AddRule<LikeOptimizationRule>();

	string input, expected_output;

	input = "S ~~ 'aaa%'";
	expected_output = "prefix(S, 'aaa')";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "S ~~ '%aaa'";
	expected_output = "suffix(S, 'aaa')";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "S ~~ '%aaa%'";
	expected_output = "contains(S, 'aaa')";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	// REQUIRE_FAIL ----------------
	input = "S ~~ 'a_a%'";
	expected_output = "prefix(S, 'aaa')";
	REQUIRE(helper.VerifyRewrite(input, expected_output, true) == false);

	input = "S ~~ '%a_a'";
	expected_output = "suffix(S, 'aaa')";
	REQUIRE(helper.VerifyRewrite(input, expected_output, true) == false);

	input = "S ~~ '%a_a%'";
	expected_output = "contains(S, 'a_a')";
	REQUIRE(helper.VerifyRewrite(input, expected_output, true) == false);
}
