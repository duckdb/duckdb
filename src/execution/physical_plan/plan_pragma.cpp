#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_pragma.hpp"

#include "duckdb/execution/operator/helper/physical_pragma.hpp"
namespace duckdb {
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalPragma &op) {
	return make_unique<PhysicalPragma>(op.function, op.info);
}

} // namespace duckdb
