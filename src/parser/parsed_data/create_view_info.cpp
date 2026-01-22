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

string CreateViewInfo::ToString() const {
	string result = GetCreatePrefix("VIEW");
	result += QualifierToString(temporary ? "" : catalog, schema, view_name);
	if (!aliases.empty()) {
		result += " (";
		result += StringUtil::Join(aliases, aliases.size(), ", ",
		                           [](const string &name) { return KeywordHelper::WriteOptionallyQuoted(name); });
		result += ")";
	}
	result += " AS ";
	result += query->ToString();
	result += ";";
	return result;
}

unique_ptr<CreateInfo> CreateViewInfo::Copy() const {
	auto result = make_uniq<CreateViewInfo>(catalog, schema, view_name);
	CopyProperties(*result);
	result->aliases = aliases;
	result->types = types;
	result->column_comments_map = column_comments_map;
	result->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
	return std::move(result);
}

unique_ptr<SelectStatement> CreateViewInfo::ParseSelect(const string &sql) {
	Parser parser;
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw BinderException(
		    "Failed to create view from SQL string - \"%s\" - statement did not contain a single SELECT statement",
		    sql);
	}
	D_ASSERT(parser.statements.size() == 1 && parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	return unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
}

unique_ptr<CreateViewInfo> CreateViewInfo::FromSelect(ClientContext &context, unique_ptr<CreateViewInfo> info) {
	D_ASSERT(info);
	D_ASSERT(!info->view_name.empty());
	D_ASSERT(!info->sql.empty());
	D_ASSERT(!info->query);

	info->query = ParseSelect(info->sql);
	return info;
}

unique_ptr<CreateViewInfo> CreateViewInfo::FromCreateView(ClientContext &context, SchemaCatalogEntry &schema,
                                                          const string &sql) {
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
	result->catalog = schema.ParentCatalog().GetName();
	result->schema = schema.name;

	auto view_binder = Binder::CreateBinder(context);
	view_binder->BindCreateViewInfo(*result);

	return result;
}

vector<Value> CreateViewInfo::GetColumnCommentsList() const {
	if (column_comments_map.empty()) {
		return vector<Value>();
	}
	if (names.empty()) {
		throw InternalException(
		    "Attempting to serialize column comments using the legacy format, but view is not bound");
	}
	vector<Value> result;
	result.resize(names.size());
	for (auto &entry : column_comments_map) {
		auto it = std::find(names.begin(), names.end(), entry.first);
		if (it == names.end()) {
			throw InternalException(
			    "While serializing comments for view \"%s\" - did not find column \"%s\" in list of names", view_name,
			    entry.first);
		}
		result[NumericCast<idx_t>(it - names.begin())] = entry.second;
	}
	return result;
}

CreateViewInfo::CreateViewInfo(vector<string> names_p, vector<Value> comments,
                               unordered_map<string, Value> column_comments_p)
    : CreateInfo(CatalogType::VIEW_ENTRY, INVALID_SCHEMA), names(std::move(names_p)),
      column_comments_map(std::move(column_comments_p)) {
	if (comments.empty()) {
		return;
	}
	if (!column_comments_map.empty()) {
		throw SerializationException("Either column_comments or column_comments_map should be provided, not both");
	}
	for (idx_t i = 0; i < comments.size(); i++) {
		if (comments[i].IsNull()) {
			continue;
		}
		column_comments_map[names[i]] = std::move(comments[i]);
	}
}

} // namespace duckdb
