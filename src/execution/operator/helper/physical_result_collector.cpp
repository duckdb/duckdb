#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/operator/helper/physical_materialized_collector.hpp"
#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"

namespace duckdb {

PhysicalResultCollector::PhysicalResultCollector(PhysicalOperator *plan_p, vector<string> names_p,
                                                 vector<LogicalType> types_p)
    : PhysicalOperator(PhysicalOperatorType::RESULT_COLLECTOR, {LogicalType::BOOLEAN}, 0), plan(plan_p),
      names(move(names_p)) {
	this->types = move(types_p);
}

unique_ptr<PhysicalResultCollector>
PhysicalResultCollector::GetResultCollector(PhysicalOperator *plan, vector<string> names, vector<LogicalType> types) {
	return make_unique<PhysicalMaterializedCollector>(plan, names, types, false);
}

vector<PhysicalOperator *> PhysicalResultCollector::GetChildren() const {
	return {plan};
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

} // namespace duckdb
