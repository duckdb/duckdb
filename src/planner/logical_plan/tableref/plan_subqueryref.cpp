#include "planner/logical_plan_generator.hpp"
#include "planner/tableref/bound_subqueryref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundSubqueryRef &ref) {
	// generate the logical plan for the subquery
	// this happens separately from the current LogicalPlan generation
	LogicalPlanGenerator generator(*ref.binder, context);
	auto subquery = generator.CreatePlan(*ref.subquery);
	return make_unique<LogicalSubquery>(move(subquery), ref.bind_index);
}
