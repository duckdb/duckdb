#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/alter_table_statement.hpp"
#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/catalog/catalog.hpp"

using namespace std;

//! This file contains the binder definitions for statements that do not need to be bound at all and only require a
//! straightforward conversion

namespace duckdb {

BoundStatement Binder::Bind(AlterTableStatement &stmt) {
	BoundStatement result;
	result.names = {"Success"};
	result.types = {SQLType::BOOLEAN};
	auto table =
	    Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context, stmt.info->schema, stmt.info->table, true);
	if (table && !table->temporary) {
		// we can only alter temporary tables in read-only mode
		this->read_only = false;
	}
	result.plan = make_unique<LogicalSimple>(LogicalOperatorType::ALTER, move(stmt.info));
	return result;
}

BoundStatement Binder::Bind(PragmaStatement &stmt) {
	BoundStatement result;
	result.names = {"Success"};
	result.types = {SQLType::BOOLEAN};
	result.plan = make_unique<LogicalSimple>(LogicalOperatorType::PRAGMA, move(stmt.info));
	return result;
}

BoundStatement Binder::Bind(TransactionStatement &stmt) {
	// transaction statements do not require a valid transaction
	this->requires_valid_transaction = false;

	BoundStatement result;
	result.names = {"Success"};
	result.types = {SQLType::BOOLEAN};
	result.plan = make_unique<LogicalSimple>(LogicalOperatorType::TRANSACTION, move(stmt.info));
	return result;
}

} // namespace duckdb
