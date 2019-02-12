#include "parser/statement/create_table_statement.hpp"
#include "planner/binder.hpp"
#include "parser/constraints/check_constraint.hpp"
#include "planner/expression_binder/check_binder.hpp"
#include "parser/expression/cast_expression.hpp"

using namespace duckdb;
using namespace std;

void Binder::Bind(CreateTableStatement &stmt) {
	// bind any constraints
	// first create a fake table
	bind_context.AddDummyTable(stmt.info->table, stmt.info->columns);
	for (auto &cond : stmt.info->constraints) {
		cond->Accept(this);
	}
}

void Binder::Visit(CheckConstraint &constraint) {
	CheckBinder binder(*this, context);
	binder.BindAndResolveType(&constraint.expression);
	// the CHECK constraint should always return an INTEGER value
	if (constraint.expression->return_type != TypeId::INTEGER) {
		constraint.expression = make_unique<CastExpression>(TypeId::INTEGER, move(constraint.expression));
	}
}
