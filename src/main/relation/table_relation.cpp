#include "duckdb/main/relation/table_relation.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/main/relation/delete_relation.hpp"
#include "duckdb/main/relation/update_relation.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

TableRelation::TableRelation(const std::shared_ptr<ClientContext> &context, unique_ptr<TableDescription> description)
    : Relation(context, RelationType::TABLE_RELATION), description(std::move(description)) {
}

unique_ptr<QueryNode> TableRelation::GetQueryNode() {
	auto result = make_unique<SelectNode>();
	result->select_list.push_back(make_unique<StarExpression>());
	result->from_table = GetTableRef();
	return std::move(result);
}

unique_ptr<TableRef> TableRelation::GetTableRef() {
	auto table_ref = make_unique<BaseTableRef>();
	table_ref->schema_name = description->schema;
	table_ref->table_name = description->table;
	return std::move(table_ref);
}

string TableRelation::GetAlias() {
	return description->table;
}

const vector<ColumnDefinition> &TableRelation::Columns() {
	return description->columns;
}

string TableRelation::ToString(idx_t depth) {
	return RenderWhitespace(depth) + "Scan Table [" + description->table + "]";
}

static unique_ptr<ParsedExpression> ParseCondition(ClientContext &context, const string &condition) {
	if (!condition.empty()) {
		auto expression_list = Parser::ParseExpressionList(condition, context.GetParserOptions());
		if (expression_list.size() != 1) {
			throw ParserException("Expected a single expression as filter condition");
		}
		return std::move(expression_list[0]);
	} else {
		return nullptr;
	}
}

void TableRelation::Update(const string &update_list, const string &condition) {
	vector<string> update_columns;
	vector<unique_ptr<ParsedExpression>> expressions;
	auto cond = ParseCondition(*context.GetContext(), condition);
	Parser::ParseUpdateList(update_list, update_columns, expressions, context.GetContext()->GetParserOptions());
	auto update = make_shared<UpdateRelation>(context, std::move(cond), description->schema, description->table,
	                                          std::move(update_columns), std::move(expressions));
	update->Execute();
}

void TableRelation::Delete(const string &condition) {
	auto cond = ParseCondition(*context.GetContext(), condition);
	auto del = make_shared<DeleteRelation>(context, std::move(cond), description->schema, description->table);
	del->Execute();
}

} // namespace duckdb
