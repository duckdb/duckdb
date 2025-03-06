#include "duckdb/execution/operator/helper/physical_vacuum.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_vacuum.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalVacuum &op) {
	auto &vacuum = Make<PhysicalVacuum>(unique_ptr_cast<ParseInfo, VacuumInfo>(std::move(op.info)), op.table,
	                                    std::move(op.column_id_map), op.estimated_cardinality);
	if (!op.children.empty()) {
		auto &plan = CreatePlan(*op.children[0]);
		vacuum.children.push_back(plan);
	}
	return vacuum;
}

} // namespace duckdb
