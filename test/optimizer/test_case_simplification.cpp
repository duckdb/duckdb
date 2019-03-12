#include "common/helper.hpp"
#include "expression_helper.hpp"
#include "optimizer/rule/case_simplification.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test case simplification", "[optimizer]") {
	DuckDB db(nullptr);
	Connection con(db);

	ExpressionHelper helper(con.context);
	helper.AddRule<CaseSimplificationRule>();

	string input, expected_output;

	input = "CASE WHEN 1=1 THEN X ELSE Y END";
	expected_output = "X";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "CASE WHEN 1=0 THEN X ELSE Y END";
	expected_output = "Y";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "CASE WHEN NULL>3 THEN X ELSE Y END";
	expected_output = "Y";
	REQUIRE(helper.VerifyRewrite(input, expected_output));
}
