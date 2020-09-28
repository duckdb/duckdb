#include "duckdb/parser/statement/alter_table_statement.hpp"
#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/planner/binder.hpp"

using namespace std;

//! This file contains the binder definitions for statements that do not need to be bound at all and only require a
//! straightforward conversion

namespace duckdb {

BoundStatement Binder::Bind(AlterStatement &stmt) {
	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	Catalog &catalog = Catalog::GetCatalog(context);
	CatalogEntry *entry;
	switch(stmt.info->type) {
	case AlterType::ALTER_TABLE: {
		auto &table_info = (AlterTableInfo &) *stmt.info;
		entry = catalog.GetEntry<TableCatalogEntry>(context, table_info.schema, table_info.table, true);
		break;
	}
	case AlterType::ALTER_VIEW: {
		auto &view_info = (AlterViewInfo &) *stmt.info;
		entry = catalog.GetEntry<ViewCatalogEntry>(context, view_info.schema, view_info.view, true);
		break;
	}
	default:
		throw InternalException("Unimplemented type for bind alter");
	}
	if (entry && !entry->temporary) {
		// we can only alter temporary tables/views in read-only mode
		this->read_only = false;
	}
	result.plan = make_unique<LogicalSimple>(LogicalOperatorType::ALTER, move(stmt.info));
	return result;
}

BoundStatement Binder::Bind(TransactionStatement &stmt) {
	// transaction statements do not require a valid transaction
	this->requires_valid_transaction = false;

	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = make_unique<LogicalSimple>(LogicalOperatorType::TRANSACTION, move(stmt.info));
	return result;
}

} // namespace duckdb
