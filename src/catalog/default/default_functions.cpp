#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"

namespace duckdb {

struct DefaultMacro {
	const char *name;
	const char *parameters[8];
	const char *macro;
};

static DefaultMacro internal_macros[] = {
	{"current_user", {nullptr}, "'duckdb'"},                       // user name of current execution context
	{"current_catalog", {nullptr}, "'duckdb'"},                    // name of current database (called "catalog" in the SQL standard)
	{"current_database", {nullptr}, "'duckdb'"},                   // name of current database
	{"user", {nullptr}, "current_user"},                           // equivalent to current_user
	{"session_user", {nullptr}, "'duckdb'"},                       // session user name
	{"inet_client_addr", {nullptr}, "NULL"},                       // address of the remote connection
	{"inet_client_port", {nullptr}, "NULL"},                       // port of the remote connection
	{"inet_server_addr", {nullptr}, "NULL"},                       // address of the local connection
	{"inet_server_port", {nullptr}, "NULL"},                       // port of the local connection
	{"pg_my_temp_schema", {nullptr}, "0"},                         // OID of session's temporary schema, or 0 if none
	{"pg_is_other_temp_schema", {"schema_id", nullptr}, "false"},  // is schema another session's temporary schema?

	{"pg_conf_load_time", {nullptr}, "current_timestamp"},         // configuration load time
	{"pg_postmaster_start_time", {nullptr}, "current_timestamp"},  // server start time

	{"pg_typeof", {"expression", nullptr}, "lower(typeof(expression))"},  // get the data type of any value

	// privilege functions
	// {"has_any_column_privilege", {"user", "table", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for any column of table
	{"has_any_column_privilege", {"table", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for any column of table
	// {"has_column_privilege", {"user", "table", "column", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for column
	{"has_column_privilege", {"table", "column", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for column
	// {"has_database_privilege", {"user", "database", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for database
	{"has_database_privilege", {"database", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for database
	// {"has_foreign_data_wrapper_privilege", {"user", "fdw", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for foreign-data wrapper
	{"has_foreign_data_wrapper_privilege", {"fdw", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for foreign-data wrapper
	// {"has_function_privilege", {"user", "function", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for function
	{"has_function_privilege", {"function", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for function
	// {"has_language_privilege", {"user", "language", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for language
	{"has_language_privilege", {"language", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for language
	// {"has_schema_privilege", {"user", "schema, privilege", nullptr}, "true"},  //boolean  //does user have privilege for schema
	{"has_schema_privilege", {"schema", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for schema
	// {"has_sequence_privilege", {"user", "sequence", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for sequence
	{"has_sequence_privilege", {"sequence", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for sequence
	// {"has_server_privilege", {"user", "server", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for foreign server
	{"has_server_privilege", {"server", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for foreign server
	// {"has_table_privilege", {"user", "table", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for table
	{"has_table_privilege", {"table", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for table
	// {"has_tablespace_privilege", {"user", "tablespace", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for tablespace
	{"has_tablespace_privilege", {"tablespace", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for tablespace

	{"pg_has_role", {"user", "role", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for role
	{"pg_has_role", {"role", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for role

	{"col_description", {"table_oid", "column_number", nullptr}, "NULL"},   // get comment for a table column
	{"obj_description", {"object_oid", "catalog_name", nullptr}, "NULL"},   // get comment for a database object
	{"shobj_description", {"object_oid", "catalog_name", nullptr}, "NULL"}, // get comment for a shared database object

	{"nullif", {"a", "b", nullptr}, "CASE WHEN a=b THEN NULL ELSE a END"},
                                         {nullptr, {nullptr}, nullptr}};

static unique_ptr<CreateFunctionInfo> GetDefaultFunction(const string &schema, const string &name) {
	for (idx_t index = 0; internal_macros[index].name != nullptr; index++) {
		if (internal_macros[index].name == name) {
			// parse the expression
			auto expressions = Parser::ParseExpressionList(internal_macros[index].macro);
			D_ASSERT(expressions.size() == 1);

			auto result = make_unique<MacroFunction>(move(expressions[0]));
			for (idx_t param_idx = 0; internal_macros[index].parameters[param_idx] != nullptr; param_idx++) {
				result->parameters.push_back(
				    make_unique<ColumnRefExpression>(internal_macros[index].parameters[param_idx]));
			}

			auto bind_info = make_unique<CreateMacroInfo>();
			bind_info->schema = DEFAULT_SCHEMA;
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
		result.push_back(internal_macros[index].name);
	}
	return result;
}

} // namespace duckdb
