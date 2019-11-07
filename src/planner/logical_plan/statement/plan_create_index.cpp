#include "duckdb/planner/logical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/statement/bound_create_index_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundCreateIndexStatement &stmt) {
	// first we visit the base table to create the root expression
	auto root = CreatePlan(*stmt.table);
	// this gives us a logical table scan
	// we take the required columns from here
	assert(root->type == LogicalOperatorType::GET);
	auto &get = (LogicalGet &)*root;
	// create the logical operator
	return make_unique<LogicalCreateIndex>(*get.table, get.column_ids, move(stmt.expressions), move(stmt.info));
}
