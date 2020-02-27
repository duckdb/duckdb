#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/statement/bound_simple_statement.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundSQLStatement> Binder::Bind(DropStatement &stmt) {
	switch (stmt.info->type) {
	case CatalogType::PREPARED_STATEMENT:
		// dropping prepared statements is always possible
		break;
	case CatalogType::SCHEMA:
		// dropping a schema is never read-only because there are no temporary schemas
		this->read_only = false;
		break;
	case CatalogType::TABLE: {
		auto table = context.catalog.GetTable(context, stmt.info->schema, stmt.info->name);
		if (!table) {
			break;
		}
		if (!table->temporary) {
			// we can only drop temporary tables in read-only mode
			this->read_only = false;
		}
		stmt.info->schema = table->schema->name;
		break;
	}
	case CatalogType::VIEW: {
		auto view = context.catalog.GetTableOrView(context, stmt.info->schema, stmt.info->name);
		if (!view) {
			// view does not exist
			break;
		}
		assert(view->type == CatalogType::VIEW);
		if (!view->temporary) {
			// we can only drop temporary views in read-only mode
			this->read_only = false;
		}
		stmt.info->schema = ((ViewCatalogEntry*)view)->schema->name;
		break;
	}
	case CatalogType::SEQUENCE: {
		auto sequence = context.catalog.GetSequence(context.ActiveTransaction(), stmt.info->schema, stmt.info->name);
		if (!sequence) {
			// sequence does not exist
			break;
		}
		if (!sequence->temporary) {
			// we can only drop temporary views in read-only mode
			this->read_only = false;
		}
		stmt.info->schema = sequence->schema->name;
		break;
	}
	case CatalogType::INDEX:
		if (stmt.info->schema == INVALID_SCHEMA) {
			stmt.info->schema = DEFAULT_SCHEMA;
		}
		this->read_only = false;
		break;
	default:
		throw BinderException("Unknown catalog type for drop statement!");
	}
	return make_unique<BoundSimpleStatement>(stmt.type, move(stmt.info));
}
