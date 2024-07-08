#include "duckdb/main/relation/query_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/parser/common_table_expression_info.hpp"

namespace duckdb {

QueryRelation::QueryRelation(const shared_ptr<ClientContext> &context, unique_ptr<SelectStatement> select_stmt_p,
                             string alias_p)
    : Relation(context, RelationType::QUERY_RELATION), select_stmt(std::move(select_stmt_p)),
      alias(std::move(alias_p)) {
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

unique_ptr<QueryNode> QueryRelation::GetQueryNode() {
	auto select = GetSelectStatement();
	return std::move(select->node);
}

unique_ptr<TableRef> QueryRelation::GetTableRef() {
	auto subquery_ref = make_uniq<SubqueryRef>(GetSelectStatement(), GetAlias());
	return std::move(subquery_ref);
}

BoundStatement QueryRelation::Bind(Binder &binder) {
	auto saved_binding_mode = binder.GetBindingMode();
	binder.SetBindingMode(BindingMode::EXTRACT_REPLACEMENT_SCANS);
	bool first_bind = columns.empty();
	auto result = Relation::Bind(binder);
	auto &replacements = binder.GetReplacementScans();
	if (first_bind) {
		auto &query_node = *select_stmt->node;
		auto &cte_map = query_node.cte_map;
		for (auto &kv : replacements) {
			auto &name = kv.first;
			auto &tableref = kv.second;

			if (!tableref->external_dependency) {
				// Only push a CTE for objects that are out of our control (i.e Python)
				// This makes sure replacement scans for files (parquet/csv/json etc) are not transformed into a CTE
				continue;
			}

			auto select = make_uniq<SelectStatement>();
			auto select_node = make_uniq<SelectNode>();
			select_node->select_list.push_back(make_uniq<StarExpression>());
			select_node->from_table = std::move(tableref);
			select->node = std::move(select_node);

			auto cte_info = make_uniq<CommonTableExpressionInfo>();
			cte_info->query = std::move(select);

			cte_map.map[name] = std::move(cte_info);
		}
	}
	replacements.clear();
	binder.SetBindingMode(saved_binding_mode);
	return result;
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
