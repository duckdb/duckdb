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
    {DEFAULT_SCHEMA, "duckdb_schemas", "SELECT * FROM duckdb_schemas() WHERE NOT internal"},
    {DEFAULT_SCHEMA, "duckdb_tables", "SELECT * FROM duckdb_tables() WHERE NOT internal"},
    {DEFAULT_SCHEMA, "duckdb_views", "SELECT * FROM duckdb_views() WHERE NOT internal"},
    {DEFAULT_SCHEMA, "pg_class", "SELECT * FROM pg_catalog.pg_class"},
    {DEFAULT_SCHEMA, "pg_depend", "SELECT * FROM pg_catalog.pg_depend"},
    {DEFAULT_SCHEMA, "pg_namespace", "SELECT * FROM pg_catalog.pg_namespace"},
    {DEFAULT_SCHEMA, "pg_sequence", "SELECT * FROM pg_catalog.pg_sequence"},
    {DEFAULT_SCHEMA, "pg_sequences", "SELECT * FROM pg_catalog.pg_sequences"},
    {DEFAULT_SCHEMA, "pg_tables", "SELECT * FROM pg_catalog.pg_tables"},
    {DEFAULT_SCHEMA, "pg_tablespace", "SELECT * FROM pg_catalog.pg_tablespace"},
    {DEFAULT_SCHEMA, "pg_views", "SELECT * FROM pg_catalog.pg_views"},
    {"pg_catalog", "pg_class", "SELECT table_oid oid, table_name relname, schema_oid relnamespace, 0 reltype, 0 reloftype, 0 relowner, 0 relam, 0 relfilenode, 0 reltablespace, 0 relpages, estimated_size::real reltuples, 0 relallvisible, 0 reltoastrelid, 0 reltoastidxid, index_count > 0 relhasindex, false relisshared, case when temporary then 't' else 'p' end relpersistence, 'r' relkind, column_count relnatts, check_constraint_count relchecks, false relhasoids, has_primary_key relhaspkey, false relhasrules, false relhastriggers, false relhassubclass, false relrowsecurity, true relispopulated, NULL relreplident, false relispartition, 0 relrewrite, 0 relfrozenxid, NULL relminmxid, NULL relacl, NULL reloptions, NULL relpartbound FROM duckdb_tables() UNION ALL SELECT view_oid oid, view_name relname, schema_oid relnamespace, 0 reltype, 0 reloftype, 0 relowner, 0 relam, 0 relfilenode, 0 reltablespace, 0 relpages, 0 reltuples, 0 relallvisible, 0 reltoastrelid, 0 reltoastidxid, false relhasindex, false relisshared, case when temporary then 't' else 'p' end relpersistence, 'v' relkind, column_count relnatts, 0 relchecks, false relhasoids, false relhaspkey, false relhasrules, false relhastriggers, false relhassubclass, false relrowsecurity, true relispopulated, NULL relreplident, false relispartition, 0 relrewrite, 0 relfrozenxid, NULL relminmxid, NULL relacl, NULL reloptions, NULL relpartbound FROM duckdb_views() UNION ALL SELECT sequence_oid oid, sequence_name relname, schema_oid relnamespace, 0 reltype, 0 reloftype, 0 relowner, 0 relam, 0 relfilenode, 0 reltablespace, 0 relpages, 0 reltuples, 0 relallvisible, 0 reltoastrelid, 0 reltoastidxid, false relhasindex, false relisshared, case when temporary then 't' else 'p' end relpersistence, 'S' relkind, 0 relnatts, 0 relchecks, false relhasoids, false relhaspkey, false relhasrules, false relhastriggers, false relhassubclass, false relrowsecurity, true relispopulated, NULL relreplident, false relispartition, 0 relrewrite, 0 relfrozenxid, NULL relminmxid, NULL relacl, NULL reloptions, NULL relpartbound FROM duckdb_sequences()"},
    {"pg_catalog", "pg_depend", "SELECT * FROM duckdb_dependencies()"},
    {"pg_catalog", "pg_namespace", "SELECT oid, schema_name nspname, 0 nspowner, NULL nspacl FROM duckdb_schemas()"},
    {"pg_catalog", "pg_sequence", "SELECT sequence_oid seqrelid, 0 seqtypid, start_value seqstart, increment_by seqincrement, max_value seqmax, min_value seqmin, 0 seqcache, cycle seqcycle FROM duckdb_sequences()"},
	{"pg_catalog", "pg_sequences", "SELECT schema_name schemaname, sequence_name sequencename, 'duckdb' sequenceowner, 0 data_type, start_value, min_value, max_value, increment_by, cycle, 0 cache_size, last_value FROM duckdb_sequences()"},
    {"pg_catalog", "pg_tables", "SELECT schema_name schemaname, table_name tablename, 'duckdb' tableowner, NULL \"tablespace\", index_count > 0 hasindexes, false hasrules, false hastriggers FROM duckdb_tables()"},
    {"pg_catalog", "pg_tablespace", "SELECT 0 oid, 'pg_default' spcname, 0 spcowner, NULL spcacl, NULL spcoptions"},
    {"pg_catalog", "pg_views", "SELECT schema_name schemaname, view_name viewname, 'duckdb' viewowner, sql definition FROM duckdb_views()"},
    {"information_schema", "columns", "SELECT * FROM information_schema_columns()"},
    {"information_schema", "schemata", "SELECT NULL catalog_name, schema_name, 'duckdb' schema_owner, NULL default_character_set_catalog, NULL default_character_set_schema, NULL default_character_set_name, sql sql_path FROM duckdb_schemas()"},
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

vector<string> DefaultViewGenerator::GetDefaultEntries() {
	vector<string> result;
	for (idx_t index = 0; internal_views[index].name != nullptr; index++) {
		if (internal_views[index].schema == schema->name) {
			result.push_back(internal_views[index].name);
		}
	}
	return result;

}

} // namespace duckdb
