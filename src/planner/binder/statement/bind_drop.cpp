#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/planner/binder.hpp"
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

	auto &properties = GetStatementProperties();
	switch (stmt.info->type) {
	case CatalogType::PREPARED_STATEMENT:
		// dropping prepared statements is always possible
		// it also does not require a valid transaction
		properties.requires_valid_transaction = false;
		break;
	case CatalogType::SCHEMA_ENTRY: {
		// dropping a schema is never read-only because there are no temporary schemas
		auto &catalog = Catalog::GetCatalog(context, stmt.info->catalog);
		properties.RegisterDBModify(catalog, context);
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
		auto catalog = Catalog::GetCatalogEntry(context, stmt.info->catalog);
		if (catalog) {
			// mark catalog as accessed
			properties.RegisterDBRead(*catalog, context);
		}
		optional_ptr<CatalogEntry> entry;
		if (stmt.info->type == CatalogType::MACRO_ENTRY) {
			// We also support "DROP MACRO" (instead of "DROP MACRO TABLE") for table macros
			// First try to drop a scalar macro
			EntryLookupInfo macro_entry_lookup(stmt.info->type, stmt.info->name);
			entry = Catalog::GetEntry(context, stmt.info->catalog, stmt.info->schema, macro_entry_lookup,
			                          OnEntryNotFound::RETURN_NULL);
			if (!entry) {
				// Unable to find a scalar macro, try to drop a table macro
				EntryLookupInfo table_macro_entry_lookup(CatalogType::TABLE_MACRO_ENTRY, stmt.info->name);
				entry = Catalog::GetEntry(context, stmt.info->catalog, stmt.info->schema, table_macro_entry_lookup,
				                          OnEntryNotFound::RETURN_NULL);
				if (entry) {
					// Change type to table macro so future lookups get the correct one
					stmt.info->type = CatalogType::TABLE_MACRO_ENTRY;
				}
			}

			if (!entry) {
				// Unable to find table macro, try again with original OnEntryNotFound to ensure we throw if necessary
				entry = Catalog::GetEntry(context, stmt.info->catalog, stmt.info->schema, macro_entry_lookup,
				                          stmt.info->if_not_found);
			}
		} else {
			EntryLookupInfo entry_lookup(stmt.info->type, stmt.info->name);
			entry = Catalog::GetEntry(context, stmt.info->catalog, stmt.info->schema, entry_lookup,
			                          stmt.info->if_not_found);
		}
		if (!entry) {
			break;
		}
		if (entry->internal) {
			throw CatalogException("Cannot drop internal catalog entry \"%s\"!", entry->name);
		}
		stmt.info->catalog = entry->ParentCatalog().GetName();
		if (!entry->temporary) {
			// we can only drop temporary schema entries in read-only mode
			properties.RegisterDBModify(entry->ParentCatalog(), context);
		}
		stmt.info->schema = entry->ParentSchema().name;
		break;
	}
	case CatalogType::SECRET_ENTRY: {
		//! Secrets are stored in the secret manager; they can always be dropped
		properties.requires_valid_transaction = false;
		break;
	}
	default:
		throw BinderException("Unknown catalog type for drop statement: '%s'", CatalogTypeToString(stmt.info->type));
	}
	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_DROP, std::move(stmt.info));
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};

	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
