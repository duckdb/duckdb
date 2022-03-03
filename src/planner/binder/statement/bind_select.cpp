#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {

BoundStatement Binder::Bind(SelectStatement &stmt) {
	this->allow_stream_result = true;
	return Bind(*stmt.node);
}

} // namespace duckdb
