#include "duckdb/execution/operator/helper/physical_execute.hpp"

namespace duckdb {

PhysicalExecute::PhysicalExecute(PhysicalOperator *plan)
    : PhysicalOperator(PhysicalOperatorType::EXECUTE, plan->types, -1), plan(plan) {
}

void PhysicalExecute::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	// EXECUTE statement: build pipeline on child
	plan->BuildPipelines(executor, current, state);
}

} // namespace duckdb
