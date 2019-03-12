#include "catch.hpp"
#include "common/helper.hpp"
#include "expression_helper.hpp"

#include "optimizer/rule/arithmetic_simplification.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Arithmetic simplification test", "[optimizer]") {
	DuckDB db(nullptr);
	Connection con(db);

	ExpressionHelper helper(con.context);
	helper.AddRule<ArithmeticSimplificationRule>();

	unique_ptr<Expression> root, result, expected_result;

	// X+0 => X
	root = helper.ParseExpression("X+0");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("X");
	REQUIRE(result->Equals(expected_result.get()));
	// 0+X => X
	root = helper.ParseExpression("0+X");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("X");
	REQUIRE(result->Equals(expected_result.get()));
	// X-0 => X
	root = helper.ParseExpression("X-0");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("X");
	REQUIRE(result->Equals(expected_result.get()));
	// X*1 => X
	root = helper.ParseExpression("X*1");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("X");
	REQUIRE(result->Equals(expected_result.get()));
	// X/0 => NULL
	root = helper.ParseExpression("X/0");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("NULL");
	REQUIRE(result->Equals(expected_result.get()));
	// X/1 => X
	root = helper.ParseExpression("X/1");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("X");
	REQUIRE(result->Equals(expected_result.get()));
}
