#include "duckdb/common/helper.hpp"
#include "expression_helper.hpp"
#include "duckdb/optimizer/rule/distributivity.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Distributivity test", "[optimizer]") {
	ExpressionHelper helper;

	REQUIRE(helper.AddColumns("A BOOLEAN, B BOOLEAN, C BOOLEAN, D BOOLEAN, X BOOLEAN, Y BOOLEAN, Z BOOLEAN").empty());
	helper.AddRule<DistributivityRule>();

	string input, expected_output;

	// "(NULL AND FALSE) OR (NULL AND TRUE)"
	// "NULL AND (FALSE OR TRUE)"

	input = "(X AND A AND B) OR (A AND X AND C) OR (X AND B AND D)";
	expected_output = "X AND ((A AND B) OR (A AND C) OR (B AND D))";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "(X AND B) OR (X AND C)";
	expected_output = "X AND (B OR C)";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "X OR X";
	expected_output = "X::BOOLEAN";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "X OR X OR X OR X";
	expected_output = "X::BOOLEAN";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "X OR (X AND A)";
	expected_output = "X AND A";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "X OR (X OR (X OR X))";
	expected_output = "X::BOOLEAN";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	// This rewrite is possible, but possibly not desirable, as we don't actually push an AND as the root expression
	// before this rewrite would be performed accidently, now we don't do it anymore, does that matter?
	// input = "((X AND A) OR (X AND B)) OR ((Y AND C) OR (Y AND D))";
	// expected_output = "(X AND (A OR B)) OR (Y AND (C OR D))";
	// REQUIRE(helper.VerifyRewrite(input, expected_output));

	REQUIRE(helper.AddColumns("X INTEGER, Y INTEGER, Z INTEGER").empty());
	input = "(X=1 AND Y=1) OR (X=1 AND Z=1)";
	expected_output = "X=1 AND (Y=1 OR Z=1)";
	REQUIRE(helper.VerifyRewrite(input, expected_output));
}
