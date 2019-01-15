#include "parser/statement/select_statement.hpp"
#include "planner/binder.hpp"

using namespace duckdb;
using namespace std;

void Binder::Bind(SelectStatement &stmt) {
	// first we visit the FROM statement
	// here we determine from where we can retrieve our columns (from which
	// tables/subqueries)

	// we also need to visit the CTEs because they generate table names
	for (auto &cte_it : stmt.cte_map) {
		AddCTE(cte_it.first, cte_it.second.get());
	}
	// now visit the root node of the select statement
	Bind(*stmt.node);
}
