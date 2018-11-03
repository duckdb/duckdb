
#include "catch.hpp"

#include <vector>

#include "expression_helper.hpp"
#include "optimizer/expression_rules/rule_list.hpp"
#include "optimizer/rewriter.hpp"
#include "parser/expression/list.hpp"

#include "common/helper.hpp"

using namespace duckdb;
using namespace std;

// (X AND B) OR (X AND C) => X AND (B OR C)
TEST_CASE("Distributivity test", "[optimizer]") {
	BindContext context;
	vector<unique_ptr<Rule>> rules;
	rules.push_back(unique_ptr<Rule>(new DistributivityRule()));
	auto rewriter = Rewriter(context, move(rules), MatchOrder::DEPTH_FIRST);

	auto root = ParseExpression("(i > 3 AND j < 5) OR (i > 3 AND k > 5)");
	auto result = ApplyExprRule(rewriter, move(root));

	REQUIRE(result->type == ExpressionType::CONJUNCTION_AND);
	int or_child =
	    result->children[0]->type == ExpressionType::CONJUNCTION_OR ? 0 : 1;
	REQUIRE(result->children[or_child]->type == ExpressionType::CONJUNCTION_OR);
}
