#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/statement/create_index_statement.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

void LogicalPlanGenerator::CreatePlan(CreateIndexStatement &statement) {
	// first we visit the base table
	statement.table->Accept(this);
	// this gives us a logical table scan
	// we take the required columns from here
	assert(root && root->type == LogicalOperatorType::GET);
	auto get = (LogicalGet *)root.get();
	auto column_ids = get->column_ids;

	// bind the table
	auto table = context.db.catalog.GetTable(context.ActiveTransaction(), statement.table->schema_name,
	                                         statement.table->table_name);
	// create the logical operator
	root = make_unique<LogicalCreateIndex>(*table, column_ids, move(statement.expressions), move(statement.info));
}
