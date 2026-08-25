#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

static bool ContainsSimpleCase(QueryNode &node);

static bool ContainsSimpleCase(const ParsedExpression &expression) {
	if (expression.GetExpressionClass() == ExpressionClass::CASE && expression.Cast<CaseExpression>().CaseOperand()) {
		return true;
	}
	if (expression.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery = expression.Cast<SubqueryExpression>().Subquery();
		if (subquery && ContainsSimpleCase(*subquery->node)) {
			return true;
		}
	}
	for (auto &child : expression.Children()) {
		if (ContainsSimpleCase(child)) {
			return true;
		}
	}
	return false;
}

static bool ContainsSimpleCase(QueryNode &node) {
	bool result = false;
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    node, [&](unique_ptr<ParsedExpression> &expression) { result |= ContainsSimpleCase(*expression); });
	return result;
}

CreateViewInfo::CreateViewInfo() : CreateInfo(CatalogType::VIEW_ENTRY, Identifier::InvalidSchema()) {
}
CreateViewInfo::CreateViewInfo(const QualifiedName &view_name)
    : CreateInfo(CatalogType::VIEW_ENTRY, view_name.Schema(), view_name.Catalog()) {
	SetViewName(view_name.Name());
}

CreateViewInfo::CreateViewInfo(SchemaCatalogEntry &schema, const Identifier &view_name)
    : CreateViewInfo(schema.GetQualifiedName(view_name)) {
}

string CreateViewInfo::ToString() const {
	string result = GetCreatePrefix("VIEW");
	result += QualifiedNameToString();
	if (!aliases.empty()) {
		result += " (";
		result +=
		    StringUtil::Join(aliases, aliases.size(), ", ", [](const Identifier &name) { return SQLIdentifier(name); });
		result += ")";
	}
	if (binding_mode == CreateViewBindingMode::SKIP_BINDING) {
		result += " WITH (DEFER_BINDING)";
	}
	result += " AS ";
	result += query->ToString();
	result += ";";
	return result;
}

unique_ptr<CreateInfo> CreateViewInfo::Copy() const {
	auto result = make_uniq<CreateViewInfo>(GetQualifiedName());
	CopyProperties(*result);
	result->aliases = aliases;
	result->types = types;
	result->names = names;
	result->column_comments_map = column_comments_map;
	result->binding_mode = binding_mode;
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
	D_ASSERT(!info->GetViewName().empty());
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
	result->SetQualifiedName(
	    QualifiedName(schema.ParentCatalog().GetName(), schema.name, result->GetQualifiedName().Name()));

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
		auto it = std::find_if(names.begin(), names.end(), [&](const Identifier &n) { return entry.first == n; });
		if (it == names.end()) {
			throw InternalException(
			    "While serializing comments for view \"%s\" - did not find column \"%s\" in list of names",
			    GetViewName(), entry.first.GetIdentifierName());
		}
		result[NumericCast<idx_t>(it - names.begin())] = entry.second;
	}
	return result;
}

unique_ptr<SelectStatement> CreateViewInfo::GetQueryForSerialization(Serializer &serializer) const {
	if (!query) {
		return nullptr;
	}
	auto result = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
	if (serializer.ShouldSerialize(StorageVersion::V2_0_0) || names.empty() || !ContainsSimpleCase(*result->node)) {
		return result;
	}

	// Preserve bound view column names when simple CASE is lowered for legacy storage.
	const Identifier query_alias("__duckdb_legacy_view_query");
	auto subquery = make_uniq<SubqueryRef>(std::move(result), query_alias);
	subquery->column_name_alias = names;

	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>(query_alias));
	select_node->from_table = std::move(subquery);

	auto wrapped_query = make_uniq<SelectStatement>();
	wrapped_query->node = std::move(select_node);
	return wrapped_query;
}

CreateViewInfo::CreateViewInfo(vector<Identifier> names_p, vector<Value> comments,
                               identifier_map_t<Value> column_comments_p)
    : CreateInfo(CatalogType::VIEW_ENTRY, Identifier::InvalidSchema()), names(std::move(names_p)),
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
