#include "duckdb/execution/operator/join/physical_positional_join.hpp"
#include "duckdb/execution/operator/scan/physical_positional_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_positional_join.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalPositionalJoin &op) {
	D_ASSERT(op.children.size() == 2);

	auto &left = CreatePlan(*op.children[0]);
	auto &right = CreatePlan(*op.children[1]);
	switch (left.type) {
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::POSITIONAL_SCAN:
		switch (right.type) {
		case PhysicalOperatorType::TABLE_SCAN:
		case PhysicalOperatorType::POSITIONAL_SCAN:
			return Make<PhysicalPositionalScan>(op.types, left, right);
		default:
			break;
		}
		break;
	default:
		break;
	}
	return Make<PhysicalPositionalJoin>(op.types, left, right, op.estimated_cardinality);
}

} // namespace duckdb
