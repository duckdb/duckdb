#include "duckdb/execution/operator/helper/physical_execute.hpp"

#include "duckdb/parallel/meta_pipeline.hpp"

namespace duckdb {

PhysicalExecute::PhysicalExecute(PhysicalOperator *plan)
    : PhysicalOperator(PhysicalOperatorType::EXECUTE, plan->types, -1), plan(plan) {
}

vector<PhysicalOperator *> PhysicalExecute::GetChildren() const {
	return {plan};
}

bool PhysicalExecute::AllOperatorsPreserveOrder() const {
	D_ASSERT(plan);
	return plan->AllOperatorsPreserveOrder();
}

void PhysicalExecute::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	// EXECUTE statement: build pipeline on child
	meta_pipeline.Build(plan);
}

} // namespace duckdb
