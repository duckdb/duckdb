#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/statement/bound_select_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundSQLStatement> Binder::Bind(SelectStatement &stmt) {
	auto result = make_unique<BoundSelectStatement>();
	// first we visit the set of CTEs and add them to the bind context
	for (auto &cte_it : stmt.cte_map) {
		AddCTE(cte_it.first, cte_it.second.get());
	}
	// now visit the root node of the select statement
	result->node = Bind(*stmt.node);
	return move(result);
}
