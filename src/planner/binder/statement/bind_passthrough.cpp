#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/passthrough_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BoundStatement Binder::Bind(PassthroughStatement &stmt) {
	if (stmt.target_is_local) {
		// `CONNECT LOCAL EXECUTE …` — the iterator already parsed the inner statement; just bind
		// it. The wrapper's only purpose was getting past the connect chokepoint.
		if (!stmt.local_statement) {
			throw InternalException("PassthroughStatement(local) carries no inner statement");
		}
		return Bind(*stmt.local_statement);
	}
	auto target_db = DatabaseManager::Get(context).GetDatabase(stmt.target);
	if (!target_db) {
		throw InvalidInputException("Database \"%s\" is not attached", stmt.target);
	}

	// Hand the verbatim payload to the target catalog. The TableRef it returns wraps the remote
	// dispatch; the local parser never tokenizes the payload.
	auto remote_ref = target_db->GetCatalog().RemoteExecute(context, stmt.payload);

	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(remote_ref);

	SelectStatement select_stmt;
	select_stmt.node = std::move(select_node);
	return Bind(select_stmt);
}

} // namespace duckdb
