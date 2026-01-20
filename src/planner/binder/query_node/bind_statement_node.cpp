#include "duckdb/parser/query_node/statement_node.hpp"
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

} // namespace duckdb
