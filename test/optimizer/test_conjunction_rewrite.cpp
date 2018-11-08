
#include "catch.hpp"

#include <vector>

#include "expression_helper.hpp"
#include "optimizer/expression_rules/rule_list.hpp"
#include "optimizer/rewriter.hpp"
#include "parser/expression/list.hpp"

#include "common/helper.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Distributivity test", "[optimizer]") {
	BindContext context;
	vector<unique_ptr<Rule>> rules;
	unique_ptr<Expression> root, result, expected_result;
	rules.push_back(unique_ptr<Rule>(new DistributivityRule()));
	auto rewriter = Rewriter(context, move(rules), MatchOrder::DEPTH_FIRST);

	// (X AND A AND B) OR (A AND X AND C) OR (X AND B AND D) => X AND ((A AND (B
	// OR C)) OR (B AND D))
	root = ParseExpression(
	    "(X AND A AND B) OR (A AND X AND C) OR (X AND B AND D)");
	result = ApplyExprRule(rewriter, move(root));
	expected_result = ParseExpression("X AND ((A AND (B OR C)) OR (B AND D))");
	REQUIRE(result->Equals(expected_result.get()));

	// (X AND B) OR (X AND C) => X AND (B OR C)
	root = ParseExpression("(X AND B) OR (X AND C)");
	result = ApplyExprRule(rewriter, move(root));
	expected_result = ParseExpression("X AND (B OR C)");
	REQUIRE(result->Equals(expected_result.get()));

	// X OR X => X
	root = ParseExpression("X OR X");
	result = ApplyExprRule(rewriter, move(root));
	expected_result = ParseExpression("X");
	REQUIRE(result->Equals(expected_result.get()));

	// X OR (X AND A) => X AND A
	root = ParseExpression("X OR (X AND A)");
	result = ApplyExprRule(rewriter, move(root));
	expected_result = ParseExpression("X AND A");
	REQUIRE(result->Equals(expected_result.get()));

	// X OR (X OR (X OR X)) => X
	root = ParseExpression("X OR (X OR (X OR X))");
	result = ApplyExprRule(rewriter, move(root));
	expected_result = ParseExpression("X");
	REQUIRE(result->Equals(expected_result.get()));

	// ((X AND A) OR (X AND B)) OR ((Y AND C) OR (Y AND D)) => (X AND (A OR B))
	// OR (Y AND (C OR D))
	root =
	    ParseExpression("((X AND A) OR (X AND B)) OR ((Y AND C) OR (Y AND D))");
	result = ApplyExprRule(rewriter, move(root));
	expected_result = ParseExpression("(X AND (A OR B)) OR (Y AND (C OR D))");
	REQUIRE(result->Equals(expected_result.get()));
}
