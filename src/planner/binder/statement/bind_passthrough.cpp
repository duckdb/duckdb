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
	// Resolve target catalog AT BIND TIME — this is what makes the "extract then ATTACH then
	// prepare" pattern work: by the time Bind runs, prior statements (including ATTACH) have
	// executed, so the target is visible.
	auto target_db = DatabaseManager::Get(context).GetDatabase(Identifier(stmt.target));
	if (!target_db) {
		throw InvalidInputException("Database \"%s\" is not attached", stmt.target);
	}
	if (!target_db->GetCatalog().Supports(RemoteCapability::CONNECT)) {
		throw InvalidInputException("Database \"%s\" does not support CONNECT", stmt.target);
	}

	// Ask the target catalog to build the TableRef that represents the remote dispatch — the
	// payload SQL travels into the catalog-supplied TableFunctionRef as a string. The local SQL
	// parser NEVER sees the payload text, so PIVOT and other "the local parser would decompose
	// this" cases survive intact.
	auto remote_ref = target_db->GetCatalog().RemoteExecute(context, stmt.payload);

	// Wrap as `SELECT * FROM <remote-ref>` and bind through the normal SELECT path.
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(remote_ref);

	SelectStatement select_stmt;
	select_stmt.node = std::move(select_node);
	return Bind(select_stmt);
}

} // namespace duckdb
