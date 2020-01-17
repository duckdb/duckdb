#include "duckdb/planner/logical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_copy_from_file.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/planner/statement/bound_copy_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundCopyStatement &stmt) {
	if (stmt.select_statement) {
		// COPY TO a file
		auto names = stmt.select_statement->names;
		auto types = stmt.select_statement->types;

		// first plan the query
		auto root = CreatePlan(*stmt.select_statement);
		// now create the copy information
		auto copy = make_unique<LogicalCopyToFile>(move(stmt.info));
		copy->AddChild(move(root));
		copy->names = names;
		copy->sql_types = types;

		return move(copy);
	} else {
		// COPY FROM a file
		assert(!stmt.info->table.empty());
		// first create a plan for the insert statement
		auto insert = CreatePlan(*stmt.bound_insert);
		// now create the copy statement and set it as a child of the insert statement
		auto copy = make_unique<LogicalCopyFromFile>(0, move(stmt.info), stmt.sql_types);
		insert->children.push_back(move(copy));
		return insert;
	}
}
