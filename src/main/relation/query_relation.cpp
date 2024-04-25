#include "duckdb/main/relation/query_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

static void InitializeTableRefDependency(TableRef &ref) {
	if (ref.type == TableReferenceType::JOIN) {
		auto &join_ref = ref.Cast<JoinRef>();
		InitializeTableRefDependency(*join_ref.right);
		InitializeTableRefDependency(*join_ref.left);
	} else {
		ref.external_dependency = make_shared_ptr<ExternalDependency>();
	}
}

QueryRelation::QueryRelation(const shared_ptr<ClientContext> &context, unique_ptr<SelectStatement> select_stmt_p,
                             string alias_p)
    : Relation(context, RelationType::QUERY_RELATION), select_stmt(std::move(select_stmt_p)),
      alias(std::move(alias_p)) {
	auto &ref = *select_stmt->node->Cast<SelectNode>().from_table;
	InitializeTableRefDependency(ref);
	context->TryBindRelation(*this, this->columns);
}

QueryRelation::~QueryRelation() {
}

unique_ptr<SelectStatement> QueryRelation::ParseStatement(ClientContext &context, const string &query,
                                                          const string &error) {
	Parser parser(context.GetParserOptions());
	parser.ParseQuery(query);
	if (parser.statements.size() != 1) {
		throw ParserException(error);
	}
	if (parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException(error);
	}
	return unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
}

unique_ptr<SelectStatement> QueryRelation::GetSelectStatement() {
	return unique_ptr_cast<SQLStatement, SelectStatement>(select_stmt->Copy());
}

BoundStatement QueryRelation::Bind(Binder &binder) {
	SelectStatement stmt;
	stmt.node = GetQueryNode();

	binder.properties.allow_stream_result = true;
	binder.properties.return_type = StatementReturnType::QUERY_RESULT;
	auto bound_node = binder.BindNode(stmt.node->Cast<SelectNode>());
	D_ASSERT(bound_node->type == QueryNodeType::SELECT_NODE);
	auto &bound_select_node = bound_node->Cast<BoundSelectNode>();
	if (bound_select_node.from_table->replacement_scan) {
		// A replacement scan took place to bind this node, replace the original with it
		auto replacement = std::move(bound_select_node.from_table->replacement_scan);
		auto &select_node = select_stmt->node->Cast<SelectNode>();
		select_node.from_table = std::move(replacement);
	}
	auto result = binder.Bind(std::move(bound_node));
	return result;
}

unique_ptr<QueryNode> QueryRelation::GetQueryNode() {
	auto select = GetSelectStatement();
	return std::move(select->node);
}

unique_ptr<TableRef> QueryRelation::GetTableRef() {
	auto subquery_ref = make_uniq<SubqueryRef>(GetSelectStatement(), GetAlias());
	return std::move(subquery_ref);
}

string QueryRelation::GetAlias() {
	return alias;
}

const vector<ColumnDefinition> &QueryRelation::Columns() {
	return columns;
}

string QueryRelation::ToString(idx_t depth) {
	return RenderWhitespace(depth) + "Subquery";
}

} // namespace duckdb
