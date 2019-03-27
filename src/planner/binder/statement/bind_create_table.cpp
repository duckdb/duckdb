#include "parser/constraints/check_constraint.hpp"
#include "parser/expression/cast_expression.hpp"
#include "parser/statement/create_table_statement.hpp"
#include "planner/binder.hpp"
#include "planner/expression_binder/check_binder.hpp"
#include "planner/statement/bound_create_table_statement.hpp"

#include "main/client_context.hpp"
#include "main/database.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundSQLStatement> Binder::Bind(CreateTableStatement &stmt) {
	auto result = make_unique<BoundCreateTableStatement>();
	if (stmt.query) {
		result->query = unique_ptr_cast<BoundSQLStatement, BoundSelectStatement>(Bind(*stmt.query));
	} else {
		// bind any constraints
		// first create a fake table
		// bind_context.AddDummyTable(stmt.info->table, stmt.info->columns);
		for (auto &cond : stmt.info->constraints) {
			if (cond->type == ConstraintType::CHECK) {
				assert(0);
				throw BinderException("Failed: binding CHECK constraints not handled yet");
			}
		}
	}
	// bind the schema
	result->schema = context.db.catalog.GetSchema(context.ActiveTransaction(), stmt.info->schema);
	result->info = move(stmt.info);
	return move(result);
}

// void Binder::Visit(CheckConstraint &constraint) {
// 	CheckBinder binder(*this, context);
// 	binder.BindAndResolveType(&constraint.expression);
// 	// the CHECK constraint should always return an INTEGER value
// 	if (constraint.expression->return_type != TypeId::INTEGER) {
// 		constraint.expression = make_unique<CastExpression>(TypeId::INTEGER, move(constraint.expression));
// 	}
// }
