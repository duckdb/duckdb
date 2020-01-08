#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/alter_table_statement.hpp"
#include "duckdb/parser/statement/create_schema_statement.hpp"
#include "duckdb/parser/statement/create_sequence_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/planner/statement/bound_simple_statement.hpp"

using namespace duckdb;
using namespace std;

//! This file contains the binder definitions for statements that do not need to be bound at all and only require a
//! straightforward conversion

unique_ptr<BoundSQLStatement> Binder::Bind(AlterTableStatement &stmt) {
	return make_unique<BoundSimpleStatement>(stmt.type, move(stmt.info));
}

unique_ptr<BoundSQLStatement> Binder::Bind(CreateSchemaStatement &stmt) {
	return make_unique<BoundSimpleStatement>(stmt.type, move(stmt.info));
}

unique_ptr<BoundSQLStatement> Binder::Bind(CreateSequenceStatement &stmt) {
	if (stmt.info->schema == INVALID_SCHEMA) {
		stmt.info->schema = DEFAULT_SCHEMA;
	}
	return make_unique<BoundSimpleStatement>(stmt.type, move(stmt.info));
}

unique_ptr<BoundSQLStatement> Binder::Bind(DropStatement &stmt) {
	return make_unique<BoundSimpleStatement>(stmt.type, move(stmt.info));
}

unique_ptr<BoundSQLStatement> Binder::Bind(PragmaStatement &stmt) {
	return make_unique<BoundSimpleStatement>(stmt.type, move(stmt.info));
}

unique_ptr<BoundSQLStatement> Binder::Bind(TransactionStatement &stmt) {
	return make_unique<BoundSimpleStatement>(stmt.type, move(stmt.info));
}
