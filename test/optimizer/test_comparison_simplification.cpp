#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "expression_helper.hpp"
#include "duckdb/optimizer/rule/comparison_simplification.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Comparison simplification test", "[optimizer]") {
	ExpressionHelper helper;

	REQUIRE(helper.AddColumns("X INTEGER").empty());

	helper.AddRule<ComparisonSimplificationRule>();

	string input, expected_output;

	input = "X=NULL";
	expected_output = "NULL";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "X>NULL";
	expected_output = "NULL";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "NULL>X";
	expected_output = "NULL";
	REQUIRE(helper.VerifyRewrite(input, expected_output));
}


TEST_CASE("Comparison simplification cast", "[optimizer]") {
	ExpressionHelper helper;

	REQUIRE(helper.AddColumns("X DATE").empty());

	helper.AddRule<ComparisonSimplificationRule>();

	string input, expected_output;

	input = "X=CAST('1994-01-01' AS DATE)";

	expected_output = "728294";
	REQUIRE(helper.VerifyRewrite(input, expected_output));
}


