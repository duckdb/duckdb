#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {

BoundStatement Binder::Bind(SelectStatement &stmt) {
	this->allow_stream_result = true;
	// first we visit the set of CTEs and add them to the bind context
	for (auto &cte_it : stmt.cte_map) {
		AddCTE(cte_it.first, cte_it.second.get());
	}
	// now visit the root node of the select statement
	return Bind(*stmt.node);
}

} // namespace duckdb
