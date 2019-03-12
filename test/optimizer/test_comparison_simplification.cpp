#include "catch.hpp"
#include "common/helper.hpp"
#include "expression_helper.hpp"
#include "optimizer/rule/comparison_simplification.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Comparison simplification test", "[optimizer]") {
	DuckDB db(nullptr);
	Connection con(db);

	ExpressionHelper helper(con.context);
	helper.AddRule<ComparisonSimplificationRule>();

	string input, expected_output;

	input = "X=NULL";
	expected_output = "NULL::BOOLEAN";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "X>NULL";
	expected_output = "NULL::BOOLEAN";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "NULL>X";
	expected_output = "NULL::BOOLEAN";
	REQUIRE(helper.VerifyRewrite(input, expected_output));
}
