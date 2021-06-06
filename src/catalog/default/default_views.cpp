#include "duckdb/catalog/default/default_views.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

namespace duckdb {

struct DefaultView {
	const char *schema;
	const char *name;
	const char *sql;
};

static DefaultView internal_views[] = {
    {DEFAULT_SCHEMA, "pragma_database_list", "SELECT * FROM pragma_database_list()"},
    {DEFAULT_SCHEMA, "sqlite_master", "SELECT * FROM sqlite_master()"},
    {DEFAULT_SCHEMA, "sqlite_schema", "SELECT * FROM sqlite_master()"},
    {DEFAULT_SCHEMA, "sqlite_temp_master", "SELECT * FROM sqlite_master()"},
    {DEFAULT_SCHEMA, "sqlite_temp_schema", "SELECT * FROM sqlite_master()"},
    {DEFAULT_SCHEMA, "pg_namespace", "SELECT * FROM pg_catalog.pg_namespace"},
    {DEFAULT_SCHEMA, "pg_depend", "SELECT * FROM pg_catalog.pg_depend"},
    {DEFAULT_SCHEMA, "pg_tablespace", "SELECT * FROM pg_catalog.pg_tablespace"},
    {"pg_catalog", "pg_depend", "SELECT * FROM duckdb_dependencies()"},
    {"pg_catalog", "pg_namespace", "SELECT id oid, schema_name nspname, 0 nspowner, NULL nspacl FROM duckdb_schemas()"},
    {"pg_catalog", "pg_tablespace", "SELECT 0 oid, 'pg_default' spcname, 0 spcowner, NULL spcacl, NULL spcoptions"},
    {"information_schema", "columns", "SELECT * FROM information_schema_columns()"},
    {"information_schema", "schemata", "SELECT catalog_name, schema_name, schema_owner, default_character_set_catalog, default_character_set_schema, default_character_set_name, sql_path FROM duckdb_schemas()"},
    {"information_schema", "tables", "SELECT * FROM information_schema_tables()"},
    {nullptr, nullptr, nullptr}};

static unique_ptr<CreateViewInfo> GetDefaultView(const string &schema, const string &name) {
	for (idx_t index = 0; internal_views[index].name != nullptr; index++) {
		if (internal_views[index].schema == schema && internal_views[index].name == name) {
			auto result = make_unique<CreateViewInfo>();
			result->schema = schema;
			result->sql = internal_views[index].sql;

			Parser parser;
			parser.ParseQuery(internal_views[index].sql);
			D_ASSERT(parser.statements.size() == 1 && parser.statements[0]->type == StatementType::SELECT_STATEMENT);
			result->query = unique_ptr_cast<SQLStatement, SelectStatement>(move(parser.statements[0]));
			result->temporary = true;
			result->internal = true;
			result->view_name = name;
			return result;
		}
	}
	return nullptr;
}

DefaultViewGenerator::DefaultViewGenerator(Catalog &catalog, SchemaCatalogEntry *schema)
    : DefaultGenerator(catalog), schema(schema) {
}

unique_ptr<CatalogEntry> DefaultViewGenerator::CreateDefaultEntry(ClientContext &context, const string &entry_name) {
	auto info = GetDefaultView(schema->name, entry_name);
	if (info) {
		auto binder = Binder::CreateBinder(context);
		binder->BindCreateViewInfo(*info);

		return make_unique_base<CatalogEntry, ViewCatalogEntry>(&catalog, schema, info.get());
	}
	return nullptr;
}

} // namespace duckdb
