#include "duckdb/catalog/default/default_views.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

struct DefaultView {
	const char *schema;
	const char *name;
	const char *sql;
};

static DefaultView internal_views[] = {
	{ DEFAULT_SCHEMA, "sqlite_master", "SELECT * FROM sqlite_master()" },
	{ "information_schema", "columns", "SELECT * FROM information_schema_columns()"},
	{ "information_schema", "schemata", "SELECT * FROM information_schema_schemata()"},
	{ "information_schema", "tables", "SELECT * FROM information_schema_tables()"},
	{ nullptr, nullptr, nullptr }
};

unique_ptr<CreateViewInfo> DefaultViews::GetDefaultView(string schema, string name) {
	for(idx_t index = 0; internal_views[index].name != nullptr; index++) {
		if (internal_views[index].schema == schema && internal_views[index].name == name) {
			auto result = make_unique<CreateViewInfo>();
			result->schema = schema;
			result->sql = internal_views[index].sql;

			Parser parser;
			parser.ParseQuery(internal_views[index].sql);
			assert(parser.statements.size() == 1 && parser.statements[0]->type == StatementType::SELECT_STATEMENT);
			result->query = move(((SelectStatement &) *parser.statements[0]).node);
			result->temporary = true;
			result->internal = true;
			result->view_name = name;
			return result;
		}
	}
	return nullptr;
}

}
