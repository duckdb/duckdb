#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {

BoundStatement Binder::Bind(SelectStatement &stmt) {
	auto &properties = GetStatementProperties();
	properties.allow_stream_result = true;
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return Bind(*stmt.node);
}

} // namespace duckdb
