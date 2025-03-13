#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_use_bf.hpp"
#include "duckdb/execution/operator/filter/physical_use_bf.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {
unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalUseBF &op) {
	auto plan = CreatePlan(*op.children[0]); // Generate child plan
	auto create_bf_op = CreatePlanFromRelated(*op.related_create_bf);
	auto &bf_plan = op.bf_to_use_plan;

	shared_ptr<BlockedBloomFilter> target_bf;
	for (auto &bf : create_bf_op->bf_to_create) {
		if (bf->column_bindings_applied_ == bf_plan->apply) {
			bf->BoundColsApplied = bf_plan->bound_cols_apply;
			target_bf = bf;
			break; // Found the target, exit loop
		}
	}

	auto use_bf = make_uniq<PhysicalUseBF>(plan->types, target_bf, create_bf_op, op.estimated_cardinality);
	use_bf->children.emplace_back(std::move(plan));
	return std::move(use_bf);
}

} // namespace duckdb
