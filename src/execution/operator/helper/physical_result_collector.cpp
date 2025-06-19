#include "duckdb/execution/operator/helper/physical_result_collector.hpp"

#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"
#include "duckdb/execution/operator/helper/physical_buffered_batch_collector.hpp"
#include "duckdb/execution/operator/helper/physical_materialized_collector.hpp"
#include "duckdb/execution/operator/helper/physical_buffered_collector.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

PhysicalResultCollector::PhysicalResultCollector(PreparedStatementData &data)
    : PhysicalOperator(PhysicalOperatorType::RESULT_COLLECTOR, {LogicalType::BOOLEAN}, 0),
      statement_type(data.statement_type), properties(data.properties), plan(data.physical_plan->Root()),
      names(data.names) {
	this->types = data.types;
}

unique_ptr<PhysicalResultCollector> PhysicalResultCollector::GetResultCollector(ClientContext &context,
                                                                                PreparedStatementData &data) {
	if (!PhysicalPlanGenerator::PreserveInsertionOrder(context, data.physical_plan->Root())) {
		// the plan is not order preserving, so we just use the parallel materialized collector
		if (data.is_streaming) {
			return make_uniq_base<PhysicalResultCollector, PhysicalBufferedCollector>(data, true);
		}
		return make_uniq_base<PhysicalResultCollector, PhysicalMaterializedCollector>(data, true);
	} else if (!PhysicalPlanGenerator::UseBatchIndex(context, data.physical_plan->Root())) {
		// the plan is order preserving, but we cannot use the batch index: use a single-threaded result collector
		if (data.is_streaming) {
			return make_uniq_base<PhysicalResultCollector, PhysicalBufferedCollector>(data, false);
		}
		return make_uniq_base<PhysicalResultCollector, PhysicalMaterializedCollector>(data, false);
	} else {
		// we care about maintaining insertion order and the sources all support batch indexes
		// use a batch collector
		if (data.is_streaming) {
			return make_uniq_base<PhysicalResultCollector, PhysicalBufferedBatchCollector>(data);
		}
		return make_uniq_base<PhysicalResultCollector, PhysicalBatchCollector>(data);
	}
}

vector<const_reference<PhysicalOperator>> PhysicalResultCollector::GetChildren() const {
	return {plan};
}

void PhysicalResultCollector::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	// operator is a sink, build a pipeline
	sink_state.reset();

	D_ASSERT(children.empty());

	// single operator: the operator becomes the data source of the current pipeline
	auto &state = meta_pipeline.GetState();
	state.SetPipelineSource(current, *this);

	// we create a new pipeline starting from the child
	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	child_meta_pipeline.Build(plan);
}

} // namespace duckdb
