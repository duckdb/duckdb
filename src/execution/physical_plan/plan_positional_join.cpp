#include "duckdb/execution/operator/join/physical_positional_join.hpp"
#include "duckdb/execution/operator/scan/physical_positional_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_positional_join.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalPositionalJoin &op) {
	D_ASSERT(op.children.size() == 2);

	auto left = CreatePlan(*op.children[0]);
	auto right = CreatePlan(*op.children[1]);
	switch (left->type) {
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::POSITIONAL_SCAN:
		switch (right->type) {
		case PhysicalOperatorType::TABLE_SCAN:
		case PhysicalOperatorType::POSITIONAL_SCAN:
			return make_uniq<PhysicalPositionalScan>(op.types, std::move(left), std::move(right));
		default:
			break;
		}
	default:
		break;
	}

	return make_uniq<PhysicalPositionalJoin>(op.types, std::move(left), std::move(right), op.estimated_cardinality);
}

} // namespace duckdb
