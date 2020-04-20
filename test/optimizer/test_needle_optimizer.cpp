#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/optimizer/rule/empty_needle_removal.hpp"
#include "expression_helper.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Empty Needle Optimization Rules", "[needle-optimizer]") {
	ExpressionHelper helper;

	REQUIRE(helper.AddColumns("S VARCHAR").empty());

	helper.AddRule<EmptyNeedleRemovalRule>();

	string input, expected_output;

	input = "PREFIX(S, '')";
	expected_output = "S";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "PREFIX(S, NULL)";
	expected_output = "NULL";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "CONTAINS(S, '')";
	expected_output = "S";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "CONTAINS(S, NULL)";
	expected_output = "NULL";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "SUFFIX(S, '')";
	expected_output = "S";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "SUFFIX(S, NULL)";
	expected_output = "NULL";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	// REQUIRE_FAIL ----------------
	input = "PREFIX(S, ' ')";
	expected_output = input;
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "PREFIX(S, 'a')";
	expected_output = input;
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "CONTAINS(S, ' ')";
	expected_output = input;
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "CONTAINS(S, 'a')";
	expected_output = input;
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "SUFFIX(S, ' ')";
	expected_output = input;
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "SUFFIX(S, 'a')";
	expected_output = input;
	REQUIRE(helper.VerifyRewrite(input, expected_output));
}
