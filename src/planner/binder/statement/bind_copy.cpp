#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/planner/statement/bound_copy_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundSQLStatement> Binder::Bind(CopyStatement &stmt) {
	auto result = make_unique<BoundCopyStatement>();
	if (stmt.select_statement) {
		// COPY from a query
		result->select_statement = Bind(*stmt.select_statement);
		result->names = {"Count"};
		result->sql_types = {SQLType(SQLTypeId::BIGINT)};
	} else {
		assert(!stmt.info->table.empty());
		// COPY to a table
		// generate an insert statement for the the to-be-inserted table
		InsertStatement insert;
		insert.table = stmt.info->table;
		insert.schema = stmt.info->schema;
		insert.columns = stmt.info->select_list;

		// bind the insert statement to the base table
		result->bound_insert = Bind(insert);
		auto &bound_insert = (BoundInsertStatement &)*result->bound_insert;
		// get the set of expected columns from the insert statement; these types will be parsed from the CSV
		result->sql_types = bound_insert.expected_types;
	}
	result->info = move(stmt.info);
	return move(result);
}
