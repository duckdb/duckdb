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

	unique_ptr<Expression> root, result, expected_result;

	// 1+1 => 2
	root = helper.ParseExpression("1+1");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("2");
	REQUIRE(result->Equals(expected_result.get()));
	// (1+1+1)*2 => 6
	root = helper.ParseExpression("(1+1+1)*2");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("6");
	REQUIRE(result->Equals(expected_result.get()));
	// 1 IN (1, 2, 3, 4) => TRUE
	root = helper.ParseExpression("1 IN (1, 2, 3, 4)");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("TRUE");
	REQUIRE(result->Equals(expected_result.get()));
	// 1 IN (1+1, 2, 3, 4) => FALSE
	root = helper.ParseExpression("1 IN (1+1, 2, 3, 4)");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("FALSE");
	REQUIRE(result->Equals(expected_result.get()));
	// 1 IN (1+1, 2, 3, 4, NULL) => NULL
	root = helper.ParseExpression("1 IN (1+1, 2, 3, 4, NULL)");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("NULL::BOOLEAN");
	REQUIRE(result->Equals(expected_result.get()));
	// CASE WHEN 1 IN (1+1, 2, 3, 4, NULL, 1) THEN (3+4) IS NULL ELSE 2+2+2>5 END
	root = helper.ParseExpression("CASE WHEN 1 IN (1+1, 2, 3, 4, NULL, 1) THEN (3+4) IS NULL ELSE 2+2+2>5 END");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("FALSE");
	REQUIRE(result->Equals(expected_result.get()));
}
