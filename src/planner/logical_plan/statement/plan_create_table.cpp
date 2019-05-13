#include "planner/logical_plan_generator.hpp"
#include "planner/operator/logical_create_table.hpp"
#include "planner/statement/bound_create_table_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundCreateTableStatement &stmt) {
	unique_ptr<LogicalOperator> root;
	if (stmt.query) {
		// create table from query
		root = CreatePlan(*stmt.query);
	}
	if (stmt.info->base->temporary) {
		throw NotImplementedException("TEMPORARY tables are not yet supported");
	}
	// create the logical operator
	auto create_table = make_unique<LogicalCreateTable>(stmt.schema, move(stmt.info));
	if (root) {
		create_table->children.push_back(move(root));
	}
	return move(create_table);
}
