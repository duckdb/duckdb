#include "parser/statement/delete_statement.hpp"
#include "planner/binder.hpp"
#include "planner/expression_binder/where_binder.hpp"

using namespace duckdb;
using namespace std;

void Binder::Bind(DeleteStatement &stmt) {
	// visit the table reference
	AcceptChild(&stmt.table);
	// project any additional columns required for the condition
	if (stmt.condition) {
		WhereBinder binder(*this, context);
		binder.BindAndResolveType(&stmt.condition);
	}
}
