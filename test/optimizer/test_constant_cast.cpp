
#include "catch.hpp"

#include <vector>

#include "optimizer/rewriter.hpp"
#include "optimizer/rules/constant_cast.hpp"
#include "parser/expression/expression_list.hpp"

using namespace duckdb;
using namespace std;

// CAST(42.0 AS INTEGER) -> 42
TEST_CASE("Constant casting does something", "[optimizer]") {

	vector<unique_ptr<OptimizerRule>> rules;
	rules.push_back(unique_ptr<OptimizerRule>(new ConstantCastRule()));
	auto rewriter = ExpressionRewriter(move(rules), MatchOrder::DEPTH_FIRST);
	auto child = make_unique<ConstantExpression>(Value(42.0));
	unique_ptr<AbstractExpression> root =
	    make_unique<CastExpression>(TypeId::INTEGER, move(child));

	auto result = rewriter.ApplyRules(move(root));
	REQUIRE(result->type == ExpressionType::VALUE_CONSTANT);
	REQUIRE(result->return_type == TypeId::INTEGER);

	auto result_cast = reinterpret_cast<ConstantExpression *>(result.get());

	REQUIRE(!result_cast->value.is_null);
	REQUIRE(result_cast->value.value_.integer == 42);

	REQUIRE(result_cast->children.size() == 0);
}
