#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/alter_table_statement.hpp"
#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/planner/statement/bound_simple_statement.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;
using namespace std;

//! This file contains the binder definitions for statements that do not need to be bound at all and only require a
//! straightforward conversion

unique_ptr<BoundSQLStatement> Binder::Bind(AlterTableStatement &stmt) {
	auto table = context.catalog.GetTable(context, stmt.info->schema, stmt.info->table);
	if (!table->temporary) {
		// we can only alter temporary tables in read-only mode
		this->read_only = false;
	}
	return make_unique<BoundSimpleStatement>(stmt.type, move(stmt.info));
}

unique_ptr<BoundSQLStatement> Binder::Bind(PragmaStatement &stmt) {
	return make_unique<BoundSimpleStatement>(stmt.type, move(stmt.info));
}

unique_ptr<BoundSQLStatement> Binder::Bind(TransactionStatement &stmt) {
	return make_unique<BoundSimpleStatement>(stmt.type, move(stmt.info));
}
