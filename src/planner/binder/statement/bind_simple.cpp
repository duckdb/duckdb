#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/alter_table_statement.hpp"
#include "duckdb/parser/statement/create_schema_statement.hpp"
#include "duckdb/parser/statement/create_sequence_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
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

unique_ptr<BoundSQLStatement> Binder::Bind(CreateSchemaStatement &stmt) {
	this->read_only = false;
	return make_unique<BoundSimpleStatement>(stmt.type, move(stmt.info));
}

unique_ptr<BoundSQLStatement> Binder::Bind(CreateSequenceStatement &stmt) {
	if (stmt.info->schema == INVALID_SCHEMA) {
		stmt.info->schema = DEFAULT_SCHEMA;
	}
	this->read_only = false;
	return make_unique<BoundSimpleStatement>(stmt.type, move(stmt.info));
}

unique_ptr<BoundSQLStatement> Binder::Bind(DropStatement &stmt) {
	switch (stmt.info->type) {
	case CatalogType::PREPARED_STATEMENT:
		// dropping prepared statements is not a read-only action
		break;
	case CatalogType::TABLE: {
		auto table = context.catalog.GetTable(context, stmt.info->schema, stmt.info->name);
		if (!table->temporary) {
			// we can only drop temporary tables in read-only mode
			this->read_only = false;
		}
		break;
	}
	default:
		this->read_only = false;
		break;
	}
	return make_unique<BoundSimpleStatement>(stmt.type, move(stmt.info));
}

unique_ptr<BoundSQLStatement> Binder::Bind(PragmaStatement &stmt) {
	return make_unique<BoundSimpleStatement>(stmt.type, move(stmt.info));
}

unique_ptr<BoundSQLStatement> Binder::Bind(TransactionStatement &stmt) {
	return make_unique<BoundSimpleStatement>(stmt.type, move(stmt.info));
}
