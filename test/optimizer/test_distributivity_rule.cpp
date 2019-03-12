#include "catch.hpp"
#include "common/helper.hpp"
#include "expression_helper.hpp"

#include "optimizer/rule/distributivity.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Distributivity test", "[optimizer]") {
	DuckDB db(nullptr);
	Connection con(db);

	ExpressionHelper helper(con.context);
	helper.AddRule<DistributivityRule>();
	
	unique_ptr<Expression> root, result, expected_result;

	// (X AND A AND B) OR (A AND X AND C) OR (X AND B AND D) => X AND ((A AND (B
	// OR C)) OR (B AND D))
	root = helper.ParseExpression("(X AND A AND B) OR (A AND X AND C) OR (X AND B AND D)");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("X AND ((A AND (B OR C)) OR (B AND D))");
	REQUIRE(result->Equals(expected_result.get()));

	// (X AND B) OR (X AND C) => X AND (B OR C)
	root = helper.ParseExpression("(X AND B) OR (X AND C)");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("X AND (B OR C)");
	REQUIRE(result->Equals(expected_result.get()));

	// X OR X => X
	root = helper.ParseExpression("X OR X");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("CAST(X AS BOOLEAN)");
	REQUIRE(result->Equals(expected_result.get()));

	// X OR X OR X OR X => X
	root = helper.ParseExpression("X OR X OR X OR X");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("CAST(X AS BOOLEAN)");
	REQUIRE(result->Equals(expected_result.get()));

	// X OR (X AND A) => X AND A
	root = helper.ParseExpression("X OR (X AND A)");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("X AND A");
	REQUIRE(result->Equals(expected_result.get()));

	// X OR (X OR (X OR X)) => X
	root = helper.ParseExpression("X OR (X OR (X OR X))");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("CAST(X AS BOOLEAN)");
	REQUIRE(result->Equals(expected_result.get()));

	// ((X AND A) OR (X AND B)) OR ((Y AND C) OR (Y AND D)) => (X AND (A OR B))
	// OR (Y AND (C OR D))
	root = helper.ParseExpression("((X AND A) OR (X AND B)) OR ((Y AND C) OR (Y AND D))");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("(X AND (A OR B)) OR (Y AND (C OR D))");
	REQUIRE(result->Equals(expected_result.get()));


	// (X=1 AND Y=1) OR (X=1 AND Z=1) => X=1 AND (Y=1 OR Z=1)
	root = helper.ParseExpression("(X=1 AND Y=1) OR (X=1 AND Z=1)");
	result = helper.ApplyExpressionRule(move(root));
	expected_result = helper.ParseExpression("X=1 AND (Y=1 OR Z=1)");
	REQUIRE(result->Equals(expected_result.get()));
}
