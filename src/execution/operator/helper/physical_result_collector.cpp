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

PhysicalResultCollector::PhysicalResultCollector(PhysicalPlan &physical_plan, PreparedStatementData &data)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::RESULT_COLLECTOR, {LogicalType::BOOLEAN}, 0),
      statement_type(data.statement_type), properties(data.properties), plan(data.physical_plan->Root()),
      names(data.names) {
	types = data.types;
}

PhysicalOperator &PhysicalResultCollector::GetResultCollector(ClientContext &context, PreparedStatementData &data) {
	auto &physical_plan = *data.physical_plan;
	auto &root = physical_plan.Root();

	if (!PhysicalPlanGenerator::PreserveInsertionOrder(context, root)) {
		// Not an order-preserving plan: use the parallel materialized collector.
		if (data.is_streaming) {
			return physical_plan.Make<PhysicalBufferedCollector>(data, true);
		}
		return physical_plan.Make<PhysicalMaterializedCollector>(data, true);
	}

	if (!PhysicalPlanGenerator::UseBatchIndex(context, root)) {
		// Order-preserving plan, and we cannot use the batch index: use single-threaded result collector.
		if (data.is_streaming) {
			return physical_plan.Make<PhysicalBufferedCollector>(data, false);
		}
		return physical_plan.Make<PhysicalMaterializedCollector>(data, false);
	}

	// Order-preserving plan, and we can use the batch index: use a batch collector.
	if (data.is_streaming) {
		return physical_plan.Make<PhysicalBufferedBatchCollector>(data);
	}
	return physical_plan.Make<PhysicalBatchCollector>(data);
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
