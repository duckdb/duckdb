#include "expression_helper.hpp"

#include "duckdb.hpp"
#include "parser/parser.hpp"
#include "parser/statement/select_statement.hpp"
#include "planner/operator/logical_projection.hpp"
#include "planner/planner.hpp"

using namespace duckdb;
using namespace std;

//! Set column ref types to a specific type (faking binding them)
static void SetColumnRefTypes(Expression &op, TypeId colref_type = TypeId::INTEGER) {
	if (op.type == ExpressionType::COLUMN_REF) {
		op.return_type = colref_type;
	}
	op.EnumerateChildren([&](Expression *child) { SetColumnRefTypes(*child, colref_type); });
}

ExpressionHelper::ExpressionHelper(ClientContext &context) : 
	context(context), rewriter(context) {

}

unique_ptr<Expression> ExpressionHelper::ParseExpression(string expression) {
	string query = "SELECT " + expression;

	Parser parser(context);
	parser.ParseQuery(query.c_str());
	if (parser.statements.size() == 0 || parser.statements[0]->type != StatementType::SELECT) {
		return nullptr;
	}
	auto &select = *((SelectStatement *)parser.statements[0].get());

	auto &select_list = select.node->GetSelectList();
	SetColumnRefTypes(*select_list[0]);
	select_list[0]->ResolveType();

	return move(select_list[0]);
}


unique_ptr<Expression> ExpressionHelper::ApplyExpressionRule(unique_ptr<Expression> root, LogicalOperatorType root_type) {
	// make a logical projection
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(move(root));
	auto proj = make_unique<LogicalProjection>(0, move(expressions));
	proj->type = root_type;
	rewriter.Apply(*proj);
	return move(proj->expressions[0]);
}

// unique_ptr<Planner> ExpressionHelper::ParseLogicalPlan(Connection &con, string query) {
// 	Parser parser(context);
// 	parser.ParseQuery(query);

// 	auto planner = make_unique<Planner>(con.context);
// 	planner->CreatePlan(move(parser.statements.back()));
// 	if (!planner->plan) {
// 		throw Exception("No plan?");
// 	}
// 	return planner;
// }
