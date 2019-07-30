#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "expression_helper.hpp"
#include "duckdb/optimizer/rule/conjunction_simplification.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Conjunction simplification test", "[optimizer]") {
	ExpressionHelper helper;

	REQUIRE(helper.AddColumns("X BOOLEAN").empty());
	helper.AddRule<ConjunctionSimplificationRule>();

	string input, expected_output;

	// input = "X AND FALSE";
	// expected_output = "FALSE";
	// REQUIRE(helper.VerifyRewrite(input, expected_output));

	// input = "FALSE AND X";
	// expected_output = "FALSE";
	// REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "X AND TRUE";
	expected_output = "X";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	// input = "X OR TRUE";
	// expected_output = "TRUE";
	// REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "X OR FALSE";
	expected_output = "X";
	REQUIRE(helper.VerifyRewrite(input, expected_output));
}
