
#include "expression_helper.hpp"

#include "duckdb.hpp"

#include "parser/parser.hpp"
#include "parser/statement/select_statement.hpp"

#include "planner/operator/logical_projection.hpp"
#include "planner/planner.hpp"

using namespace std;

namespace duckdb {

//! Set column ref types to a specific type (faking binding them)
static void SetColumnRefTypes(Expression &op,
                              TypeId colref_type = TypeId::INTEGER) {
	if (op.type == ExpressionType::COLUMN_REF) {
		op.return_type = colref_type;
	}
	for (auto &child : op.children) {
		SetColumnRefTypes(*child, colref_type);
	}
}

unique_ptr<Expression> ParseExpression(string expression) {
	string query = "SELECT " + expression;

	Parser parser;
	if (!parser.ParseQuery(query.c_str())) {
		return nullptr;
	}
	if (parser.statements.size() == 0 ||
	    parser.statements[0]->type != StatementType::SELECT) {
		return nullptr;
	}
	auto &select = *((SelectStatement *)parser.statements[0].get());

	SetColumnRefTypes(*select.select_list[0]);
	select.select_list[0]->ResolveType();

	return move(select.select_list[0]);
}

unique_ptr<Expression> ApplyExprRule(Rewriter &rewriter,
                                     unique_ptr<Expression> root) {
	vector<unique_ptr<Expression>> exprs;
	exprs.push_back(move(root));

	auto op = make_unique<LogicalProjection>(move(exprs));

	auto result = rewriter.ApplyRules(move(op));
	return move(result->expressions[0]);
}

unique_ptr<Planner> ParseLogicalPlan(DuckDBConnection &con, string query) {
	Parser parser;
	if (!parser.ParseQuery(query)) {
		throw Exception(parser.GetErrorMessage());
	}

	auto planner = make_unique<Planner>();
	if (!planner->CreatePlan(con.context, move(parser.statements.back()))) {
		throw Exception(planner->GetErrorMessage());
	}
	if (!planner->plan) {
		throw Exception("No plan?");
	}
	return planner;
}

} // namespace duckdb
