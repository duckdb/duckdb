#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/constraints/check_constraint.hpp"
#include "parser/expression/cast_expression.hpp"
#include "parser/statement/create_table_statement.hpp"
#include "planner/binder.hpp"
#include "planner/constraints/bound_check_constraint.hpp"
#include "planner/expression_binder/check_binder.hpp"
#include "planner/expression_binder/constant_binder.hpp"
#include "planner/statement/bound_create_table_statement.hpp"

using namespace duckdb;
using namespace std;

void Binder::BindConstraints(string table, vector<ColumnDefinition> &columns,
                             vector<unique_ptr<Constraint>> &constraints) {
	CheckBinder binder(*this, context, table, columns);
	for (uint64_t i = 0; i < constraints.size(); i++) {
		auto &cond = constraints[i];
		if (cond->type == ConstraintType::CHECK) {
			auto &check = (CheckConstraint &)*cond;
			auto unbound_constraint = make_unique<CheckConstraint>(check.expression->Copy());
			auto condition = binder.Bind(check.expression);
			constraints[i] = make_unique<BoundCheckConstraint>(move(condition), move(unbound_constraint));
		}
	}
}

unique_ptr<BoundSQLStatement> Binder::Bind(CreateTableStatement &stmt) {
	auto result = make_unique<BoundCreateTableStatement>();
	if (stmt.query) {
		result->query = unique_ptr_cast<BoundSQLStatement, BoundSelectStatement>(Bind(*stmt.query));
	} else {
		// bind any constraints
		BindConstraints(stmt.info->table, stmt.info->columns, stmt.info->constraints);
		// bind the default values
		for (uint64_t i = 0; i < stmt.info->columns.size(); i++) {
			unique_ptr<Expression> bound_default;
			if (stmt.info->columns[i].default_value) {
				// we bind a copy of the DEFAULT value because binding is destructive
				// and we want to keep the original expression around for serialization
				auto default_copy = stmt.info->columns[i].default_value->Copy();
				ConstantBinder default_binder(*this, context, "DEFAULT value");
				default_binder.target_type = stmt.info->columns[i].type;
				bound_default = default_binder.Bind(default_copy);
			}
			stmt.info->bound_defaults.push_back(move(bound_default));
		}
	}
	// bind the schema
	result->schema = context.catalog.GetSchema(context.ActiveTransaction(), stmt.info->schema);
	result->info = move(stmt.info);
	return move(result);
}
