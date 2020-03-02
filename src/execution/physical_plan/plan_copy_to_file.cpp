#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCopyToFile &op) {
	auto plan = CreatePlan(*op.children[0]);
	// COPY from select statement to file
	auto copy = make_unique<PhysicalCopyToFile>(op, move(op.info));
	copy->names = op.names;
	copy->sql_types = op.sql_types;

	copy->children.push_back(move(plan));
	return move(copy);
}
