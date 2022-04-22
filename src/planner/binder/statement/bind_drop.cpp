#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

BoundStatement Binder::Bind(DropStatement &stmt) {
	BoundStatement result;

	switch (stmt.info->type) {
	case CatalogType::PREPARED_STATEMENT:
		// dropping prepared statements is always possible
		// it also does not require a valid transaction
		this->requires_valid_transaction = false;
		break;
	case CatalogType::SCHEMA_ENTRY:
		// dropping a schema is never read-only because there are no temporary schemas
		this->read_only = false;
		break;
	case CatalogType::VIEW_ENTRY:
	case CatalogType::SEQUENCE_ENTRY:
	case CatalogType::MACRO_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY:
	case CatalogType::INDEX_ENTRY:
	case CatalogType::TABLE_ENTRY:
	case CatalogType::TYPE_ENTRY: {
		auto entry = (StandardEntry *)Catalog::GetCatalog(context).GetEntry(context, stmt.info->type, stmt.info->schema,
		                                                                    stmt.info->name, true);
		if (!entry) {
			break;
		}
		if (!entry->temporary) {
			// we can only drop temporary tables in read-only mode
			this->read_only = false;
		}
		stmt.info->schema = entry->schema->name;
		break;
	}
	default:
		throw BinderException("Unknown catalog type for drop statement!");
	}
	result.plan = make_unique<LogicalSimple>(LogicalOperatorType::LOGICAL_DROP, move(stmt.info));
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	this->allow_stream_result = false;
	return result;
}

} // namespace duckdb
