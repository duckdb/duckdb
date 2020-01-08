#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "expression_helper.hpp"
#include "duckdb/optimizer/rule/constant_folding.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Constant folding test", "[optimizer]") {
	ExpressionHelper helper;
	helper.AddRule<ConstantFoldingRule>();

	string input, expected_output;

	input = "1+1";
	expected_output = "2";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "(1+1+1)*2";
	expected_output = "6";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "CASE WHEN 1 IN (1, 2, 3, 4) THEN 3 ELSE 5 END";
	expected_output = "3";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "CASE WHEN 1 IN (1+1, 2, 3, 4) THEN 3 ELSE 5 END";
	expected_output = "5";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "1 IN (1+1, 2, 3, 4, NULL)";
	expected_output = "NULL";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "CASE WHEN 1 IN (1+1, 2, 3, 4, NULL, 1) THEN (3+4) ELSE 2+2+2 END";
	expected_output = "7";
	REQUIRE(helper.VerifyRewrite(input, expected_output));
}
