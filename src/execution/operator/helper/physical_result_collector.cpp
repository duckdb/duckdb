#include "duckdb/execution/operator/helper/physical_result_collector.hpp"

namespace duckdb {

PhysicalResultCollector::PhysicalResultCollector(PhysicalOperator *plan_p)
	: PhysicalOperator(PhysicalOperatorType::RESULT_COLLECTOR, {LogicalType::BOOLEAN}, 0), plan(plan_p) {
}

void PhysicalResultCollector::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	// operator is a sink, build a pipeline
	sink_state.reset();

	// single operator:
	// the operator becomes the data source of the current pipeline
	state.SetPipelineSource(current, this);
	// we create a new pipeline starting from the child
	D_ASSERT(children.size() == 0);
	D_ASSERT(plan);

	BuildChildPipeline(executor, current, state, plan);
}

}
