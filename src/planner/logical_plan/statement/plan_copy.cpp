#include "planner/logical_plan_generator.hpp"
#include "planner/operator/logical_copy.hpp"
#include "planner/statement/bound_copy_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundCopyStatement &stmt) {
	if (stmt.select_statement) {
		// COPY from a query
		// first plan the query
		auto root = CreatePlan(*stmt.select_statement);
		// now create the copy information
		auto copy = make_unique<LogicalCopy>(nullptr, move(stmt.info));
		copy->names = root->GetNames();
		copy->AddChild(move(root));
		return move(copy);
	} else {
		// COPY to a table
		assert(!stmt.info->table.empty());
		return make_unique<LogicalCopy>(stmt.table, move(stmt.info));
	}
}
