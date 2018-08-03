
#include "catch.hpp"

#include <vector>

#include "optimizer/rewriter.hpp"
#include "optimizer/rules/constant_folding.hpp"
#include "parser/expression/expression_list.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Constant folding does something", "[optimizer]") {

	vector<unique_ptr<OptimizerRule>> rules;
	rules.push_back(unique_ptr<OptimizerRule>(new ConstantFoldingRule()));
	auto rewriter = ExpressionRewriter(move(rules), MatchOrder::DEPTH_FIRST);

	auto left = make_unique<ConstantExpression>(Value(42));
	auto right = make_unique<ConstantExpression>(Value(1));

	unique_ptr<AbstractExpression> root = make_unique<OperatorExpression>(
	    ExpressionType::OPERATOR_ADD, TypeId::INTEGER, move(left), move(right));
	auto result = rewriter.ApplyRules(move(root));

	REQUIRE(result->type == ExpressionType::VALUE_CONSTANT);

	auto result_cast = reinterpret_cast<ConstantExpression *>(result.get());

	REQUIRE(result_cast->value.value_.integer == 43);
	REQUIRE(result_cast->children.size() == 0);
}

TEST_CASE("Constant folding finishes in fixpoint", "[optimizer]") {

	vector<unique_ptr<OptimizerRule>> rules;
	rules.push_back(unique_ptr<OptimizerRule>(new ConstantFoldingRule()));
	auto rewriter = ExpressionRewriter(move(rules), MatchOrder::DEPTH_FIRST);

	auto ll = make_unique<ConstantExpression>(Value(42));
	auto lr = make_unique<ConstantExpression>(Value(1));

	unique_ptr<AbstractExpression> il = make_unique<OperatorExpression>(
	    ExpressionType::OPERATOR_ADD, TypeId::INTEGER, move(ll), move(lr));
	auto ir = make_unique<ConstantExpression>(Value(10));
	unique_ptr<AbstractExpression> root = make_unique<OperatorExpression>(
	    ExpressionType::OPERATOR_ADD, TypeId::INTEGER, move(il), move(ir));

	auto result = rewriter.ApplyRules(move(root));

	REQUIRE(result->type == ExpressionType::VALUE_CONSTANT);

	auto result_cast = reinterpret_cast<ConstantExpression *>(result.get());

	REQUIRE(result_cast->value.value_.integer == 53);
	REQUIRE(result_cast->children.size() == 0);
}
