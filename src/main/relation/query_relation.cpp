#include "duckdb/main/relation/query_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

QueryRelation::QueryRelation(ClientContext &context, string query, string alias)
    : Relation(context, RelationType::QUERY_RELATION), query(move(query)), alias(move(alias)) {
	context.TryBindRelation(*this, this->columns);
}

unique_ptr<SelectStatement> QueryRelation::GetSelectStatement() {
	Parser parser(context.GetParserOptions());
	parser.ParseQuery(query);
	if (parser.statements.size() != 1) {
		throw ParserException("Expected a single SELECT statement");
	}
	if (parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("Expected a single SELECT statement");
	}
	return unique_ptr_cast<SQLStatement, SelectStatement>(move(parser.statements[0]));
}

unique_ptr<QueryNode> QueryRelation::GetQueryNode() {
	auto select = GetSelectStatement();
	return move(select->node);
}

unique_ptr<TableRef> QueryRelation::GetTableRef() {
	auto subquery_ref = make_unique<SubqueryRef>(GetSelectStatement(), GetAlias());
	return move(subquery_ref);
}

string QueryRelation::GetAlias() {
	return alias;
}

const vector<ColumnDefinition> &QueryRelation::Columns() {
	return columns;
}

string QueryRelation::ToString(idx_t depth) {
	return RenderWhitespace(depth) + "Subquery [" + query + "]";
}

} // namespace duckdb
