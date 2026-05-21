#include "duckdb/planner/statement_rewriter.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"

namespace duckdb {

StatementRewriter::StatementRewriter(ClientContext &context) : context(context) {
}

void StatementRewriter::FindRemoteCatalogsInSearchPath() {
	auto &client_data = ClientData::Get(context);
	// iterate over all catalogs mentioned in the search path and check if they are remote
	auto search_path = client_data.catalog_search_path->Get();
	for(auto &entry : search_path) {
	    auto catalog_entry = Catalog::GetCatalogEntry(context, entry.catalog);
		if (!catalog_entry) {
			continue;
		}
		if (!catalog_entry->IsRemoteCatalog()) {
			// not a remote catalog
			continue;
		}
		remote_catalogs_in_search_path.push_back(*catalog_entry);
	}
}

optional_ptr<QueryNode> TryGetQueryNode(SQLStatement &stmt) {
	switch(stmt.type) {
	case StatementType::INSERT_STATEMENT: {
		auto &insert = stmt.Cast<InsertStatement>();
		return insert.node.get();
	}
	case StatementType::DELETE_STATEMENT: {
		auto &delete_stmt = stmt.Cast<DeleteStatement>();
		return delete_stmt.node.get();
	}
	case StatementType::UPDATE_STATEMENT: {
		auto &update = stmt.Cast<UpdateStatement>();
		return update.node.get();
	}
	case StatementType::SELECT_STATEMENT: {
		auto &select = stmt.Cast<SelectStatement>();
		return select.node.get();
	}
	default:
		return nullptr;
	}
}

struct RemotePushdownResult {
	RemotePushdownResult() {}

	optional_ptr<Catalog> catalog;
	unique_ptr<QueryNode> new_node;

	void FinalizePushdown(QueryNode &node);
};


void RemotePushdownResult::FinalizePushdown(QueryNode &node) {
	if (!catalog) {
		// no catalog at this layer - no pushdown
		return;
	}
	// we pushed down at this layer - generate the new node
	string sql;
	if (new_node) {
		// use the new node
		sql = new_node->ToString();
	} else {
		sql = node.ToString();
	}
	auto function_name = catalog->GetRemoteExecuteFunction();

	// create a node that is "SELECT * FROM <remote_function>(<catalog_name>, <sql>)
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	vector<unique_ptr<ParsedExpression>> function_children;
	function_children.emplace_back(make_uniq<ConstantExpression>(Value(catalog->GetName())));
	function_children.emplace_back(make_uniq<ConstantExpression>(Value(std::move(sql))));
	auto function_ref = make_uniq<FunctionExpression>(function_name, std::move(function_children));

	auto table_function = make_uniq<TableFunctionRef>();
	table_function->function = std::move(function_ref);

	select_node->from_table = std::move(table_function);
	new_node = std::move(select_node);
}

RemotePushdownResult StatementRewriter::TryPushdown(ParsedExpression &ref) {
	return RemotePushdownResult();
}
RemotePushdownResult StatementRewriter::TryPushdown(TableRef &ref) {
	switch(ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base_table = ref.Cast<BaseTableRef>();
		// figure out if this base table refers to a remote catalog
		// FIXME: need to check if this is a CTE reference
//		base_table.catalog_name
//		base_table.schema_name
//		base_table.table_name
	}

	}
	return RemotePushdownResult();
}

RemotePushdownResult StatementRewriter::TryPushdown(QueryNode &node) {
	switch(node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		// first try to pushdown into the table ref
		auto ref_pushdown = TryPushdown(*select.from_table);
		if (ref_pushdown.catalog) {

		}

		break;
	}
	default:
		break;
	}
	return RemotePushdownResult();
}

unique_ptr<SQLStatement> StatementRewriter::Rewrite(SQLStatement &statement) {
	auto node = TryGetQueryNode(statement);
	if (!node) {
		// we cannot push this statement down
		return nullptr;
	}
	FindRemoteCatalogsInSearchPath();
	auto result = TryPushdown(*node);
	result.FinalizePushdown(*node);
	if (result.new_node) {
		auto result_stmt = make_uniq<SelectStatement>();
		result_stmt->node = std::move(result.new_node);
		return std::move(result_stmt);
	}
	// traverse the SQLStatement
	return nullptr;
}

}
