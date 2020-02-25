#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/create_view_statement.hpp"
#include "duckdb/planner/statement/bound_simple_statement.hpp"
#include "duckdb/planner/bound_query_node.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundSQLStatement> Binder::Bind(CreateViewStatement &stmt) {
	// bind the view as if it were a query so we can catch errors
	// note that we bind a copy and don't actually use the bind result
	auto copy = stmt.info->query->Copy();
	auto query_node = Bind(*copy);
	this->read_only = false;
	if (stmt.info->aliases.size() > query_node->names.size()) {
		throw BinderException("More VIEW aliases than columns in query result");
	}
	return make_unique<BoundSimpleStatement>(stmt.type, move(stmt.info));
}
