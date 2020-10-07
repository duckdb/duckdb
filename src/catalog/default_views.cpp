#include "duckdb/catalog/default_views.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

struct DefaultView {
	const char *name;
	const char *sql;
};

static DefaultView internal_views[] = {
	{ "sqlite_master", "SELECT * FROM sqlite_master()" },
	{ nullptr, nullptr }
};

unique_ptr<CreateViewInfo> DefaultViews::GetDefaultView(string name) {
	for(idx_t index = 0; internal_views[index].name != nullptr; index++) {
		if (internal_views[index].name == name) {
			auto result = make_unique<CreateViewInfo>();
			result->schema = DEFAULT_SCHEMA;
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
