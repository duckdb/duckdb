#include "duckdb/planner/logical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/tableref/bound_dummytableref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundEmptyTableRef &ref) {
	return make_unique<LogicalGet>(ref.bind_index);
}
