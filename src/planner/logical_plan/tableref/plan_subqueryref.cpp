#include "duckdb/planner/logical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_subquery.hpp"
#include "duckdb/planner/tableref/bound_subqueryref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundSubqueryRef &ref) {
	// generate the logical plan for the subquery
	// this happens separately from the current LogicalPlan generation
	LogicalPlanGenerator generator(*ref.binder, context);
	generator.plan_subquery = plan_subquery;
	auto subquery = generator.CreatePlan(*ref.subquery);
	if (generator.has_unplanned_subqueries) {
		has_unplanned_subqueries = true;
	}
	return make_unique<LogicalSubquery>(move(subquery), ref.bind_index);
}
