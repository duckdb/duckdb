#include "parser/statement/copy_statement.hpp"
#include "planner/statement/bound_copy_statement.hpp"
#include "planner/binder.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundSQLStatement> Binder::Bind(CopyStatement &stmt) {
	auto result = make_unique<BoundCopyStatement>();
	if (stmt.select_statement) {
		// COPY from a query
		result->select_statement = Bind(*select_statement);
	} else {
		assert(!statement.info->table.empty());
		// COPY to a table
		result->table = context.db.catalog.GetTable(context.ActiveTransaction(), stmt.info->schema, stmt.info->table);
	}
	result->info = move(stmt.info);
	return move(result);
}
