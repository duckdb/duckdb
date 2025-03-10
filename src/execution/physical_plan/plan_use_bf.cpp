#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_use_bf.hpp"
#include "duckdb/execution/operator/filter/physical_use_bf.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {
unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalUseBF &op) {
	unique_ptr<PhysicalOperator> plan = CreatePlan(*op.children[0]);

	vector<shared_ptr<BlockedBloomFilter>> bfs;
	vector<PhysicalCreateBF *> physical_related_create_bfs;
	auto create_bf_op = CreatePlanFromRelated(*op.related_create_bf);
	for (auto &bf : create_bf_op->bf_to_create) {
		auto &bf_plan = op.bf_to_use_plan;
		if (bf->GetColApplied() == bf_plan->apply) {
			bf->BoundColsApplied = bf_plan->bound_cols_apply;
			bfs.push_back(bf);
			break;
		}
	}
	physical_related_create_bfs.push_back(create_bf_op);

	auto use_bf = make_uniq<PhysicalUseBF>(plan->types, bfs, physical_related_create_bfs, op.estimated_cardinality);
	use_bf->children.emplace_back(std::move(plan));
	plan = std::move(use_bf);
	return plan;
}
} // namespace duckdb
