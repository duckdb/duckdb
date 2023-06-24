#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

BoundStatement Binder::Bind(DropStatement &stmt) {
	BoundStatement result;

	switch (stmt.info->type) {
	case CatalogType::PREPARED_STATEMENT:
		// dropping prepared statements is always possible
		// it also does not require a valid transaction
		properties.requires_valid_transaction = false;
		break;
	case CatalogType::SCHEMA_ENTRY: {
		// dropping a schema is never read-only because there are no temporary schemas
		auto &catalog = Catalog::GetCatalog(context, stmt.info->catalog);
		properties.modified_databases.insert(catalog.GetName());
		break;
	}
	case CatalogType::VIEW_ENTRY:
	case CatalogType::SEQUENCE_ENTRY:
	case CatalogType::MACRO_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY:
	case CatalogType::INDEX_ENTRY:
	case CatalogType::TABLE_ENTRY:
	case CatalogType::TYPE_ENTRY: {
		BindSchemaOrCatalog(stmt.info->catalog, stmt.info->schema);
		auto entry = Catalog::GetEntry(context, stmt.info->type, stmt.info->catalog, stmt.info->schema, stmt.info->name,
		                               OnEntryNotFound::RETURN_NULL);
		if (!entry) {
			break;
		}
		stmt.info->catalog = entry->ParentCatalog().GetName();
		if (!entry->temporary) {
			// we can only drop temporary tables in read-only mode
			properties.modified_databases.insert(stmt.info->catalog);
		}
		stmt.info->schema = entry->ParentSchema().name;
		break;
	}
	default:
		throw BinderException("Unknown catalog type for drop statement!");
	}
	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_DROP, std::move(stmt.info));
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
