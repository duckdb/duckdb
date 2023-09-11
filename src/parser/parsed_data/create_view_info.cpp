#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"

namespace duckdb {

CreateViewInfo::CreateViewInfo() : CreateInfo(CatalogType::VIEW_ENTRY, INVALID_SCHEMA) {
}
CreateViewInfo::CreateViewInfo(string catalog_p, string schema_p, string view_name_p)
    : CreateInfo(CatalogType::VIEW_ENTRY, std::move(schema_p), std::move(catalog_p)),
      view_name(std::move(view_name_p)) {
}

CreateViewInfo::CreateViewInfo(SchemaCatalogEntry &schema, string view_name)
    : CreateViewInfo(schema.catalog.GetName(), schema.name, std::move(view_name)) {
}

unique_ptr<CreateInfo> CreateViewInfo::Copy() const {
	auto result = make_uniq<CreateViewInfo>(catalog, schema, view_name);
	CopyProperties(*result);
	result->aliases = aliases;
	result->types = types;
	result->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
	return std::move(result);
}

unique_ptr<CreateViewInfo> CreateViewInfo::FromSelect(ClientContext &context, unique_ptr<CreateViewInfo> info) {
	D_ASSERT(info);
	D_ASSERT(!info->view_name.empty());
	D_ASSERT(!info->sql.empty());
	D_ASSERT(!info->query);

	Parser parser;
	parser.ParseQuery(info->sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw BinderException(
		    "Failed to create view from SQL string - \"%s\" - statement did not contain a single SELECT statement",
		    info->sql);
	}
	D_ASSERT(parser.statements.size() == 1 && parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	info->query = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));

	auto binder = Binder::CreateBinder(context);
	binder->BindCreateViewInfo(*info);

	return info;
}

unique_ptr<CreateViewInfo> CreateViewInfo::FromCreateView(ClientContext &context, const string &sql) {
	D_ASSERT(!sql.empty());

	// parse the SQL statement
	Parser parser;
	parser.ParseQuery(sql);

	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::CREATE_STATEMENT) {
		throw BinderException(
		    "Failed to create view from SQL string - \"%s\" - statement did not contain a single CREATE VIEW statement",
		    sql);
	}
	auto &create_statement = parser.statements[0]->Cast<CreateStatement>();
	if (create_statement.info->type != CatalogType::VIEW_ENTRY) {
		throw BinderException(
		    "Failed to create view from SQL string - \"%s\" - view did not contain a CREATE VIEW statement", sql);
	}

	auto result = unique_ptr_cast<CreateInfo, CreateViewInfo>(std::move(create_statement.info));

	auto binder = Binder::CreateBinder(context);
	binder->BindCreateViewInfo(*result);

	return result;
}

} // namespace duckdb
