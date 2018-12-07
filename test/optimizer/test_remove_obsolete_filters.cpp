#include "catch.hpp"
#include "common/helper.hpp"
#include "expression_helper.hpp"
#include "optimizer/logical_rules/list.hpp"
#include "optimizer/rewriter.hpp"
#include "parser/expression/list.hpp"
#include "planner/operator/logical_filter.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

TEST_CASE("Test remove obsolete filter rule", "[optimizer]") {
	BindContext context;
	vector<unique_ptr<Rule>> rules;
	unique_ptr<Expression> root, expected_result;
	unique_ptr<LogicalOperator> result;
	rules.push_back(unique_ptr<Rule>(new RemoveObsoleteFilterRule()));
	auto rewriter = Rewriter(context, move(rules), MatchOrder::DEPTH_FIRST);
	auto filter = make_unique<LogicalFilter>();

	filter->expressions.push_back(ParseExpression("(X > 5)"));
	filter->expressions.push_back(ParseExpression("(X > 7)"));
	result = ApplyLogicalRule(rewriter, move(filter));
	expected_result = ParseExpression("X > 7");
	REQUIRE(result->expressions.size() == 1);
	REQUIRE(result->expressions[0]->Equals(expected_result.get()));

	filter = make_unique<LogicalFilter>();
	filter->expressions.push_back(ParseExpression("(X > 5)"));
	filter->expressions.push_back(ParseExpression("(X = 7)"));
	result = ApplyLogicalRule(rewriter, move(filter));
	expected_result = ParseExpression("X = 7");
	REQUIRE(result->expressions.size() == 1);
	REQUIRE(result->expressions[0]->Equals(expected_result.get()));

	filter = make_unique<LogicalFilter>();
	filter->expressions.push_back(ParseExpression("(X < 5)"));
	filter->expressions.push_back(ParseExpression("(X <= 5)"));
	result = ApplyLogicalRule(rewriter, move(filter));
	expected_result = ParseExpression("X < 5");
	REQUIRE(result->expressions.size() == 1);
	REQUIRE(result->expressions[0]->Equals(expected_result.get()));

	filter = make_unique<LogicalFilter>();
	filter->expressions.push_back(ParseExpression("(X >= 5)"));
	filter->expressions.push_back(ParseExpression("(X > 5)"));
	result = ApplyLogicalRule(rewriter, move(filter));
	expected_result = ParseExpression("X > 5");
	REQUIRE(result->expressions.size() == 1);
	REQUIRE(result->expressions[0]->Equals(expected_result.get()));

	filter = make_unique<LogicalFilter>();
	filter->expressions.push_back(ParseExpression("(X < 5)"));
	filter->expressions.push_back(ParseExpression("(X > 5)"));
	result = ApplyLogicalRule(rewriter, move(filter));
	REQUIRE(result->expressions.size() == 2);
}
