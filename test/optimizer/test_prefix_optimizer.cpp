#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/optimizer/rule/empty_prefix_removal.hpp"
#include "expression_helper.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Prefix Optimization Rules", "[prefix-optimizer]") {
	ExpressionHelper helper;

	REQUIRE(helper.AddColumns("S VARCHAR").empty());

	helper.AddRule<EmptyPrefixRemovalRule>();

	string input, expected_output;

	input = "PREFIX(S, '')";
	expected_output = "S";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "PREFIX(S, NULL)";
	expected_output = "NULL";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	// REQUIRE_FAIL ----------------
	input = "PREFIX(S, ' ')";
	expected_output = input;
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "PREFIX(S, 'a')";
	expected_output = input;
	REQUIRE(helper.VerifyRewrite(input, expected_output));
}
