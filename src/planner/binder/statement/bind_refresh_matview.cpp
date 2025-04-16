#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/statement/refresh_matview_statement.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {
BoundStatement Binder::Bind(RefreshMatViewStatement &stmt) {
	BoundStatement result;
	BindSchemaOrCatalog(stmt.info->catalog, stmt.info->schema);
	auto catalog = Catalog::GetCatalogEntry(context, stmt.info->catalog);
	auto &properties = GetStatementProperties();
	if (catalog) {
		// mark catalog as accessed
		properties.RegisterDBRead(*catalog, context);
	}
	EntryLookupInfo entry_lookup(stmt.info->type, stmt.info->name);
	auto entry = Catalog::GetEntry(context, stmt.info->catalog, stmt.info->schema, entry_lookup,
				       OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		break;
	}
	stmt.info->catalog = entry->ParentCatalog().GetName();
	if (!entry->temporary) {
		// we can only drop temporary schema entries in read-only mode
		properties.RegisterDBModify(entry->ParentCatalog(), context);
	}
	stmt.info->schema = entry->ParentSchema().name;
	return result;
}
} // namespace duckdb
