#include "duckdb/planner/logical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/statement/bound_insert_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundInsertStatement &stmt) {
	auto table = stmt.table;

	auto insert = make_unique<LogicalInsert>(table, move(stmt.bound_defaults));
	insert->column_index_map = stmt.column_index_map;
	if (stmt.select_statement) {
		// insert from select statement
		// parse select statement and add to logical plan
		auto root = CreatePlan(*stmt.select_statement);
		root = CastLogicalOperatorToTypes(stmt.select_statement->node->types, stmt.expected_types, move(root));
		insert->AddChild(move(root));
	}
	return move(insert);
}
