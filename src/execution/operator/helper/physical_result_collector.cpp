#include "duckdb/execution/operator/helper/physical_result_collector.hpp"

#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"
#include "duckdb/execution/operator/helper/physical_materialized_collector.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {

PhysicalResultCollector::PhysicalResultCollector(PreparedStatementData &data)
    : PhysicalOperator(PhysicalOperatorType::RESULT_COLLECTOR, {LogicalType::BOOLEAN}, 0),
      statement_type(data.statement_type), properties(data.properties), plan(data.plan.get()), names(data.names) {
	this->types = data.types;
}

unique_ptr<PhysicalResultCollector> PhysicalResultCollector::GetResultCollector(ClientContext &context,
                                                                                PreparedStatementData &data) {
	if (!PhysicalPlanGenerator::PreserveInsertionOrder(context, *data.plan)) {
		// the plan is not order preserving, so we just use the parallel materialized collector
		return make_unique_base<PhysicalResultCollector, PhysicalMaterializedCollector>(data, true);
	} else if (!PhysicalPlanGenerator::UseBatchIndex(context, *data.plan)) {
		// the plan is order preserving, but we cannot use the batch index: use a single-threaded result collector
		return make_unique_base<PhysicalResultCollector, PhysicalMaterializedCollector>(data, false);
	} else {
		// we care about maintaining insertion order and the sources all support batch indexes
		// use a batch collector
		return make_unique_base<PhysicalResultCollector, PhysicalBatchCollector>(data);
	}
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
