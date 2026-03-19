#include "duckdb/parser/query_node/statement_node.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BoundStatement Binder::BindNode(StatementNode &statement) {
	// switch on type here to ensure we bind WITHOUT ctes to prevent infinite recursion
	switch (statement.stmt.type) {
	case StatementType::INSERT_STATEMENT:
		return Bind(statement.stmt.Cast<InsertStatement>());
	case StatementType::DELETE_STATEMENT:
		return Bind(statement.stmt.Cast<DeleteStatement>());
	case StatementType::UPDATE_STATEMENT:
		return Bind(statement.stmt.Cast<UpdateStatement>());
	case StatementType::MERGE_INTO_STATEMENT:
		return Bind(statement.stmt.Cast<MergeIntoStatement>());
	default:
		return Bind(statement.stmt);
	}
}

BoundStatement Binder::BindNode(InsertQueryNode &node) {
	InsertStatement stmt;
	stmt.table = node.table;
	stmt.schema = node.schema;
	stmt.catalog = node.catalog;
	stmt.columns = node.columns;
	stmt.default_values = node.default_values;
	stmt.column_order = node.column_order;
	if (node.select_statement) {
		stmt.select_statement = unique_ptr_cast<SQLStatement, SelectStatement>(node.select_statement->Copy());
	}
	for (auto &expr : node.returning_list) {
		stmt.returning_list.push_back(expr->Copy());
	}
	if (node.on_conflict_info) {
		stmt.on_conflict_info = node.on_conflict_info->Copy();
	}
	if (node.table_ref) {
		stmt.table_ref = node.table_ref->Copy();
	}
	return Bind(stmt);
}

BoundStatement Binder::BindNode(UpdateQueryNode &node) {
	UpdateStatement stmt;
	if (node.table) {
		stmt.table = node.table->Copy();
	}
	if (node.from_table) {
		stmt.from_table = node.from_table->Copy();
	}
	for (auto &expr : node.returning_list) {
		stmt.returning_list.push_back(expr->Copy());
	}
	if (node.set_info) {
		stmt.set_info = node.set_info->Copy();
	}
	return Bind(stmt);
}

BoundStatement Binder::BindNode(DeleteQueryNode &node) {
	DeleteStatement stmt;
	if (node.condition) {
		stmt.condition = node.condition->Copy();
	}
	if (node.table) {
		stmt.table = node.table->Copy();
	}
	for (auto &clause : node.using_clauses) {
		stmt.using_clauses.push_back(clause->Copy());
	}
	for (auto &expr : node.returning_list) {
		stmt.returning_list.push_back(expr->Copy());
	}
	return Bind(stmt);
}

} // namespace duckdb
