#include "catch.hpp"
#include "common/helper.hpp"
#include "expression_helper.hpp"
#include "optimizer/rule/constant_folding.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Constant folding test", "[optimizer]") {
	DuckDB db(nullptr);
	Connection con(db);

	ExpressionHelper helper(con.context);
	helper.AddRule<ConstantFoldingRule>();

	string input, expected_output;

	input = "1+1";
	expected_output = "2";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "(1+1+1)*2";
	expected_output = "6";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "1 IN (1, 2, 3, 4)";
	expected_output = "TRUE";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "1 IN (1+1, 2, 3, 4)";
	expected_output = "FALSE";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "1 IN (1+1, 2, 3, 4, NULL)";
	expected_output = "NULL::BOOLEAN";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "CASE WHEN 1 IN (1+1, 2, 3, 4, NULL, 1) THEN (3+4) IS NULL ELSE 2+2+2>5 END";
	expected_output = "FALSE";
	REQUIRE(helper.VerifyRewrite(input, expected_output));
}
