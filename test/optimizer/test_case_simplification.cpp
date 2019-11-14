#include "duckdb/common/helper.hpp"
#include "expression_helper.hpp"
#include "duckdb/optimizer/rule/case_simplification.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test case simplification", "[optimizer]") {
	ExpressionHelper helper;

	REQUIRE(helper.AddColumns("X INTEGER").empty());

	helper.AddRule<CaseSimplificationRule>();

	string input, expected_output;

	input = "CASE WHEN 1=1 THEN X+1 ELSE X+2 END";
	expected_output = "X+1";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "CASE WHEN 1=0 THEN X+1 ELSE X+2 END";
	expected_output = "X+2";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "CASE WHEN NULL>3 THEN X+1 ELSE X+2 END";
	expected_output = "X+2";
	REQUIRE(helper.VerifyRewrite(input, expected_output));
}
