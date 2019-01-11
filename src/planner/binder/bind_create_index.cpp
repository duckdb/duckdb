#include "parser/statement/create_index_statement.hpp"
#include "planner/binder.hpp"

using namespace duckdb;
using namespace std;

void Binder::Bind(CreateIndexStatement &stmt) {
	// visit the table reference
	AcceptChild(&stmt.table);
	// visit the expressions
	for (auto &expr : stmt.expressions) {
		VisitExpression(&expr);
		expr->ResolveType();
		if (expr->return_type == TypeId::INVALID) {
			throw BinderException("Could not resolve type of projection element!");
		}
		expr->ClearStatistics();
	}
}
