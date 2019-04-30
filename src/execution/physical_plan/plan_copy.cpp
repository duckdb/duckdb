#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "execution/operator/persistent/physical_copy.hpp"
#include "execution/physical_plan_generator.hpp"
#include "planner/operator/logical_copy.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCopy &op) {
	if (op.children.size() > 0) {
		auto plan = CreatePlan(*op.children[0]);
		// COPY from select statement
		assert(!op.table);
		auto copy = make_unique<PhysicalCopy>(op, move(op.info));
		copy->names = op.names;
		copy->children.push_back(move(plan));
		return move(copy);
	} else {
		// COPY to table
		dependencies.insert(op.table);
		return make_unique<PhysicalCopy>(op, op.table, move(op.info));
	}
}
