#include "parser/statement/update_statement.hpp"
#include "planner/binder.hpp"
#include "planner/expression_binder/update_binder.hpp"
#include "planner/expression_binder/where_binder.hpp"

using namespace duckdb;
using namespace std;

void Binder::Bind(UpdateStatement &stmt) {
	// visit the table reference
	AcceptChild(&stmt.table);
	// project any additional columns required for the condition/expressions
	if (stmt.condition) {
		WhereBinder binder(*this, context);
		binder.BindAndResolveType(&stmt.condition);
	}
	for (auto &expression : stmt.expressions) {
		if (expression->type == ExpressionType::VALUE_DEFAULT) {
			// we resolve the type of the DEFAULT expression in the
			// LogicalPlanGenerator because that is where we resolve the
			// to-be-updated column
			continue;
		}
		UpdateBinder binder(*this, context);
		binder.BindAndResolveType(&expression);
	}
}
