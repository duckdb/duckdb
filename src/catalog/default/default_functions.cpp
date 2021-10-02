#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"

namespace duckdb {

struct DefaultMacro {
	const char *schema;
	const char *name;
	const char *parameters[8];
	const char *macro;
};

static DefaultMacro internal_macros[] = {
	{DEFAULT_SCHEMA, "current_user", {nullptr}, "'duckdb'"},                       // user name of current execution context
	{DEFAULT_SCHEMA, "current_catalog", {nullptr}, "'duckdb'"},                    // name of current database (called "catalog" in the SQL standard)
	{DEFAULT_SCHEMA, "current_database", {nullptr}, "'duckdb'"},                   // name of current database
	{DEFAULT_SCHEMA, "user", {nullptr}, "current_user"},                           // equivalent to current_user
	{DEFAULT_SCHEMA, "session_user", {nullptr}, "'duckdb'"},                       // session user name
	{"pg_catalog", "inet_client_addr", {nullptr}, "NULL"},                       // address of the remote connection
	{"pg_catalog", "inet_client_port", {nullptr}, "NULL"},                       // port of the remote connection
	{"pg_catalog", "inet_server_addr", {nullptr}, "NULL"},                       // address of the local connection
	{"pg_catalog", "inet_server_port", {nullptr}, "NULL"},                       // port of the local connection
	{"pg_catalog", "pg_my_temp_schema", {nullptr}, "0"},                         // OID of session's temporary schema, or 0 if none
	{"pg_catalog", "pg_is_other_temp_schema", {"schema_id", nullptr}, "false"},  // is schema another session's temporary schema?

	{"pg_catalog", "pg_conf_load_time", {nullptr}, "current_timestamp"},         // configuration load time
	{"pg_catalog", "pg_postmaster_start_time", {nullptr}, "current_timestamp"},  // server start time

	{"pg_catalog", "pg_typeof", {"expression", nullptr}, "lower(typeof(expression))"},  // get the data type of any value

	// privilege functions
	// {"has_any_column_privilege", {"user", "table", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for any column of table
	{"pg_catalog", "has_any_column_privilege", {"table", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for any column of table
	// {"has_column_privilege", {"user", "table", "column", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for column
	{"pg_catalog", "has_column_privilege", {"table", "column", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for column
	// {"has_database_privilege", {"user", "database", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for database
	{"pg_catalog", "has_database_privilege", {"database", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for database
	// {"has_foreign_data_wrapper_privilege", {"user", "fdw", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for foreign-data wrapper
	{"pg_catalog", "has_foreign_data_wrapper_privilege", {"fdw", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for foreign-data wrapper
	// {"has_function_privilege", {"user", "function", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for function
	{"pg_catalog", "has_function_privilege", {"function", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for function
	// {"has_language_privilege", {"user", "language", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for language
	{"pg_catalog", "has_language_privilege", {"language", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for language
	// {"has_schema_privilege", {"user", "schema, privilege", nullptr}, "true"},  //boolean  //does user have privilege for schema
	{"pg_catalog", "has_schema_privilege", {"schema", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for schema
	// {"has_sequence_privilege", {"user", "sequence", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for sequence
	{"pg_catalog", "has_sequence_privilege", {"sequence", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for sequence
	// {"has_server_privilege", {"user", "server", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for foreign server
	{"pg_catalog", "has_server_privilege", {"server", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for foreign server
	// {"has_table_privilege", {"user", "table", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for table
	{"pg_catalog", "has_table_privilege", {"table", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for table
	// {"has_tablespace_privilege", {"user", "tablespace", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for tablespace
	{"pg_catalog", "has_tablespace_privilege", {"tablespace", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for tablespace

	// various postgres system functions
	{"pg_catalog", "pg_get_viewdef", {"oid", nullptr}, "(select sql from duckdb_views() v where v.view_oid=oid)"},
	{"pg_catalog", "pg_get_constraintdef", {"constraint_oid", "pretty_bool", nullptr}, "(select constraint_text from duckdb_constraints() d_constraint where d_constraint.table_oid=constraint_oid/1000000 and d_constraint.constraint_index=constraint_oid%1000000)"},
	{"pg_catalog", "pg_get_expr", {"pg_node_tree", "relation_oid", nullptr}, "pg_node_tree"},
	{"pg_catalog", "format_pg_type", {"type_name", nullptr}, "case when type_name='FLOAT' then 'real' when type_name='DOUBLE' then 'double precision' when type_name='DECIMAL' then 'numeric' when type_name='VARCHAR' then 'character varying' when type_name='BLOB' then 'bytea' when type_name='TIMESTAMP' then 'timestamp without time zone' when type_name='TIME' then 'time without time zone' else lower(type_name) end"},
	{"pg_catalog", "format_type", {"type_oid", "typemod", nullptr}, "(select format_pg_type(type_name) from duckdb_types() t where t.type_oid=type_oid) || case when typemod>0 then concat('(', typemod/1000, ',', typemod%1000, ')') else '' end"},

	{"pg_catalog", "pg_has_role", {"user", "role", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for role
	{"pg_catalog", "pg_has_role", {"role", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for role

	{"pg_catalog", "col_description", {"table_oid", "column_number", nullptr}, "NULL"},   // get comment for a table column
	{"pg_catalog", "obj_description", {"object_oid", "catalog_name", nullptr}, "NULL"},   // get comment for a database object
	{"pg_catalog", "shobj_description", {"object_oid", "catalog_name", nullptr}, "NULL"}, // get comment for a shared database object

	// visibility functions
	{"pg_catalog", "pg_collation_is_visible", {"collation_oid", nullptr}, "true"},
	{"pg_catalog", "pg_conversion_is_visible", {"conversion_oid", nullptr}, "true"},
	{"pg_catalog", "pg_function_is_visible", {"function_oid", nullptr}, "true"},
	{"pg_catalog", "pg_opclass_is_visible", {"opclass_oid", nullptr}, "true"},
	{"pg_catalog", "pg_operator_is_visible", {"operator_oid", nullptr}, "true"},
	{"pg_catalog", "pg_opfamily_is_visible", {"opclass_oid", nullptr}, "true"},
	{"pg_catalog", "pg_table_is_visible", {"table_oid", nullptr}, "true"},
	{"pg_catalog", "pg_ts_config_is_visible", {"config_oid", nullptr}, "true"},
	{"pg_catalog", "pg_ts_dict_is_visible", {"dict_oid", nullptr}, "true"},
	{"pg_catalog", "pg_ts_parser_is_visible", {"parser_oid", nullptr}, "true"},
	{"pg_catalog", "pg_ts_template_is_visible", {"template_oid", nullptr}, "true"},
	{"pg_catalog", "pg_type_is_visible", {"type_oid", nullptr}, "true"},

	{DEFAULT_SCHEMA, "round_even", {"x", "n", nullptr}, "CASE ((abs(x) * power(10, n+1)) % 10) WHEN 5 THEN round(x/2, n) * 2 ELSE round(x, n) END"},
	{DEFAULT_SCHEMA, "roundbankers", {"x", "n", nullptr}, "round_even(x, n)"},
	{DEFAULT_SCHEMA, "nullif", {"a", "b", nullptr}, "CASE WHEN a=b THEN NULL ELSE a END"},
    {nullptr, nullptr, {nullptr}, nullptr}};

static unique_ptr<CreateFunctionInfo> GetDefaultFunction(const string &schema, const string &name) {
	for (idx_t index = 0; internal_macros[index].name != nullptr; index++) {
		if (internal_macros[index].schema == schema && internal_macros[index].name == name) {
			// parse the expression
			auto expressions = Parser::ParseExpressionList(internal_macros[index].macro);
			D_ASSERT(expressions.size() == 1);

			auto result = make_unique<MacroFunction>(move(expressions[0]));
			for (idx_t param_idx = 0; internal_macros[index].parameters[param_idx] != nullptr; param_idx++) {
				result->parameters.push_back(
				    make_unique<ColumnRefExpression>(internal_macros[index].parameters[param_idx]));
			}

			auto bind_info = make_unique<CreateMacroInfo>();
			bind_info->schema = schema;
			bind_info->name = internal_macros[index].name;
			bind_info->temporary = true;
			bind_info->internal = true;
			bind_info->function = move(result);
			return move(bind_info);
		}
	}
	return nullptr;
}

DefaultFunctionGenerator::DefaultFunctionGenerator(Catalog &catalog, SchemaCatalogEntry *schema)
    : DefaultGenerator(catalog), schema(schema) {
}

unique_ptr<CatalogEntry> DefaultFunctionGenerator::CreateDefaultEntry(ClientContext &context,
                                                                      const string &entry_name) {
	auto info = GetDefaultFunction(schema->name, entry_name);
	if (info) {
		return make_unique_base<CatalogEntry, MacroCatalogEntry>(&catalog, schema, (CreateMacroInfo *)info.get());
	}
	return nullptr;
}

vector<string> DefaultFunctionGenerator::GetDefaultEntries() {
	vector<string> result;
	for (idx_t index = 0; internal_macros[index].name != nullptr; index++) {
		if (internal_macros[index].schema == schema->name) {
			result.emplace_back(internal_macros[index].name);
		}
	}
	return result;
}

} // namespace duckdb
