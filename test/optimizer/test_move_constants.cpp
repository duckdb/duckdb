#include "duckdb/common/helper.hpp"
#include "expression_helper.hpp"
#include "duckdb/optimizer/rule/constant_folding.hpp"
#include "duckdb/optimizer/rule/move_constants.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test move constants", "[optimizer]") {
	ExpressionHelper helper;

	REQUIRE(helper.AddColumns("X INTEGER").empty());
	helper.AddRule<ConstantFoldingRule>();
	helper.AddRule<MoveConstantsRule>();

	string input, expected_output;

	// addition
	input = "X+1=10";
	expected_output = "X=9";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "1+X=10";
	expected_output = "X=9";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "10=X+1";
	expected_output = "9=X";
	REQUIRE(helper.VerifyRewrite(input, expected_output));
	// subtraction
	input = "X-1=10";
	expected_output = "X=11";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "10-X=5";
	expected_output = "X=5";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "10-X<5";
	expected_output = "X>5";
	REQUIRE(helper.VerifyRewrite(input, expected_output));

	input = "10-X>=5";
	expected_output = "X<=5";
	REQUIRE(helper.VerifyRewrite(input, expected_output));
	// multiplication
	input = "3*X=6";
	expected_output = "X=2";
	REQUIRE(helper.VerifyRewrite(input, expected_output));
	input = "X*2=8";
	expected_output = "X=4";
	REQUIRE(helper.VerifyRewrite(input, expected_output));
	input = "X*3>3";
	expected_output = "X>1";
	REQUIRE(helper.VerifyRewrite(input, expected_output));
	// multiplication by negative value
	input = "-1*X=-5";
	expected_output = "X=5";
	REQUIRE(helper.VerifyRewrite(input, expected_output));
	input = "-1*X<-5";
	expected_output = "X>5";
	REQUIRE(helper.VerifyRewrite(input, expected_output));
	// negation, FIXME:
	// input = "-X=-5";
	// expected_output = "X=5";
	// REQUIRE(helper.VerifyRewrite(input, expected_output));
	// input = "-X<-5";
	// expected_output = "X>5";
	// REQUIRE(helper.VerifyRewrite(input, expected_output));
}
