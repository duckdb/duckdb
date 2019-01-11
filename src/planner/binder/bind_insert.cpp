#include "parser/statement/insert_statement.hpp"
#include "planner/binder.hpp"

using namespace duckdb;
using namespace std;

void Binder::Bind(InsertStatement &statement) {
	if (statement.select_statement) {
		Bind(*statement.select_statement);
	}
	// visit the expressions
	for (auto &expression_list : statement.values) {
		for (auto &expression : expression_list) {
			VisitExpression(&expression);
			expression->ResolveType();
		}
	}
}
