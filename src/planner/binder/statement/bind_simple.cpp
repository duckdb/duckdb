#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/planner/binder.hpp"

//! This file contains the binder definitions for statements that do not need to be bound at all and only require a
//! straightforward conversion

namespace duckdb {

BoundStatement Binder::Bind(AlterStatement &stmt) {
	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	BindSchemaOrCatalog(stmt.info->catalog, stmt.info->schema);
	auto entry = Catalog::GetEntry(context, stmt.info->GetCatalogType(), stmt.info->catalog, stmt.info->schema,
	                               stmt.info->name, stmt.info->if_not_found);
	if (entry) {
		auto &catalog = entry->ParentCatalog();
		if (!entry->temporary) {
			// we can only alter temporary tables/views in read-only mode
			properties.modified_databases.insert(catalog.GetName());
		}
		stmt.info->catalog = catalog.GetName();
		stmt.info->schema = entry->ParentSchema().name;
	}
	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_ALTER, std::move(stmt.info));
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

BoundStatement Binder::Bind(TransactionStatement &stmt) {
	// transaction statements do not require a valid transaction
	properties.requires_valid_transaction = stmt.info->type == TransactionType::BEGIN_TRANSACTION;

	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_TRANSACTION, std::move(stmt.info));
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
