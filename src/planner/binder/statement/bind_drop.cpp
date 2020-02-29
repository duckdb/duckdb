#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/statement/bound_simple_statement.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

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
	case CatalogType::VIEW:
	case CatalogType::SEQUENCE:
	case CatalogType::INDEX:
	case CatalogType::TABLE: {
		auto entry = (StandardEntry*) Catalog::GetCatalog(context).GetEntry(context, stmt.info->type, stmt.info->schema, stmt.info->name, true);
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
	return make_unique<BoundSimpleStatement>(stmt.type, move(stmt.info));
}
