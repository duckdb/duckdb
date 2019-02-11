#include "parser/statement/create_table_statement.hpp"
#include "planner/binder.hpp"

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
