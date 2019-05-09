#include "expression_helper.hpp"

#include "duckdb.hpp"
#include "optimizer/rule/constant_folding.hpp"
#include "parser/parser.hpp"
#include "planner/binder.hpp"
#include "planner/bound_query_node.hpp"
#include "planner/expression_iterator.hpp"
#include "planner/operator/logical_projection.hpp"
#include "planner/planner.hpp"
#include "planner/statement/bound_select_statement.hpp"

using namespace duckdb;
using namespace std;

ExpressionHelper::ExpressionHelper() : db(nullptr), con(db), rewriter(*con.context) {
	con.Query("BEGIN TRANSACTION");
}

bool ExpressionHelper::VerifyRewrite(string input, string expected_output) {
	auto root = ParseExpression(input);
	auto result = ApplyExpressionRule(move(root));
	auto expected_result = ParseExpression(expected_output);
	bool equals = Expression::Equals(result.get(), expected_result.get());
	if (!equals) {
		printf("Optimized result does not equal expected result!\n");
		result->Print();
		printf("Expected:\n");
		expected_result->Print();
	}
	return equals;
}

string ExpressionHelper::AddColumns(string columns) {
	if (!from_clause.empty()) {
		con.Query("DROP TABLE expression_helper");
	}
	auto result = con.Query("CREATE TABLE expression_helper(" + columns + ")");
	if (!result->success) {
		return result->error;
	}
	from_clause = " FROM expression_helper";
	return string();
}

unique_ptr<Expression> ExpressionHelper::ParseExpression(string expression) {
	string query = "SELECT " + expression + from_clause;

	Parser parser(*con.context);
	parser.ParseQuery(query.c_str());
	if (parser.statements.size() == 0 || parser.statements[0]->type != StatementType::SELECT) {
		return nullptr;
	}
	Binder binder(*con.context);
	auto bound_statement = binder.Bind(*parser.statements[0]);

	auto &select_list =
	    (vector<unique_ptr<Expression>> &)((BoundSelectStatement &)*bound_statement).node->GetSelectList();
	return move(select_list[0]);
}

unique_ptr<LogicalOperator> ExpressionHelper::ParseLogicalTree(string query) {
	Parser parser(*con.context);
	parser.ParseQuery(query.c_str());
	if (parser.statements.size() == 0 || parser.statements[0]->type != StatementType::SELECT) {
		return nullptr;
	}
	Planner planner(*con.context);
	planner.CreatePlan(move(parser.statements[0]));
	return move(planner.plan);
}

unique_ptr<Expression> ExpressionHelper::ApplyExpressionRule(unique_ptr<Expression> root) {
	// make a logical projection
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(move(root));
	auto proj = make_unique<LogicalProjection>(0, move(expressions));
	rewriter.Apply(*proj);
	return move(proj->expressions[0]);
}
