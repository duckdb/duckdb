#include "catch.hpp"
#include "common/helper.hpp"
#include "expression_helper.hpp"
#include "optimizer/rule/conjunction_simplification.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Conjunction simplification test", "[optimizer]") {
	DuckDB db(nullptr);
	Connection con(db);

	ExpressionHelper helper(con.context);
	helper.AddRule<ConjunctionSimplificationRule>();

	string input, expected_output;

	input = "X AND FALSE";
	expected_output = "FALSE";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "FALSE AND X";
	expected_output = "FALSE";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "X AND TRUE";
	expected_output = "X::BOOLEAN";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "X OR TRUE";
	expected_output = "TRUE";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "X OR FALSE";
	expected_output = "X::BOOLEAN";
	REQUIRE(helper.VerifyRewrite(input, expected_output));
}
