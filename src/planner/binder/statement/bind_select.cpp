#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

BoundStatement Binder::Bind(SelectStatement &stmt) {
	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::ALLOW_STREAMING;
	properties.return_type = StatementReturnType::QUERY_RESULT;
	auto result = Bind(*stmt.node);
	// Generalize #23358 from CALL to the bare `SELECT * FROM func()` / `FROM func()` form: when the
	// statement is just a table-function passthrough, honor the function's call_return_type so a
	// CONNECT-routed statement with no result set (DDL/SET) is reported as its true return type and
	// displays like a native statement. Only overrides when the function opts in (non-default modifier),
	// so normal queries are unaffected.
	if (result.plan) {
		auto get = GetPassthroughTableFunctionGet(*result.plan);
		if (get && get->function.call_return_type != StatementReturnType::QUERY_RESULT) {
			properties.return_type = get->function.call_return_type;
		}
	}
	return result;
}

} // namespace duckdb
