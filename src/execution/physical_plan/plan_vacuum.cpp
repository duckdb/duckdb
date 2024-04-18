#include "duckdb/execution/operator/helper/physical_vacuum.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_vacuum.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalVacuum &op) {
	auto result = make_uniq<PhysicalVacuum>(unique_ptr_cast<ParseInfo, VacuumInfo>(std::move(op.info)), op.table,
	                                        std::move(op.column_id_map), op.estimated_cardinality);
	if (!op.children.empty()) {
		auto child = CreatePlan(*op.children[0]);
		result->children.push_back(std::move(child));
	}
	return std::move(result);
}

} // namespace duckdb
