#include "planner/logical_plan_generator.hpp"
#include "planner/operator/logical_get.hpp"
#include "planner/tableref/bound_dummytableref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundDummyTableRef &ref) {
	return make_unique<LogicalGet>(ref.bind_index);
}
