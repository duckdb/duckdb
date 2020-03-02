#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/planner/statement/bound_explain_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundSQLStatement> Binder::Bind(ExplainStatement &stmt) {
	auto result = make_unique<BoundExplainStatement>();
	// bind the underlying statement
	result->bound_statement = Bind(*stmt.stmt);
	return move(result);
}
