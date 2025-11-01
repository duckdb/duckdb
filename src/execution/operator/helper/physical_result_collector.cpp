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
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

PhysicalResultCollector::PhysicalResultCollector(PhysicalPlan &physical_plan, PreparedStatementData &data)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::RESULT_COLLECTOR, {LogicalType::BOOLEAN}, 0),
      statement_type(data.statement_type), properties(data.properties), memory_type(data.memory_type),
      plan(data.physical_plan->Root()), names(data.names) {
	types = data.types;
}

PhysicalOperator &PhysicalResultCollector::GetResultCollector(ClientContext &context, PreparedStatementData &data) {
	auto &physical_plan = *data.physical_plan;
	auto &root = physical_plan.Root();

	if (!PhysicalPlanGenerator::PreserveInsertionOrder(context, root)) {
		// Not an order-preserving plan: use the parallel materialized collector.
		if (data.output_type == QueryResultOutputType::ALLOW_STREAMING) {
			return physical_plan.Make<PhysicalBufferedCollector>(data, true);
		}
		return physical_plan.Make<PhysicalMaterializedCollector>(data, true);
	}

	if (!PhysicalPlanGenerator::UseBatchIndex(context, root)) {
		// Order-preserving plan, and we cannot use the batch index: use single-threaded result collector.
		if (data.output_type == QueryResultOutputType::ALLOW_STREAMING) {
			return physical_plan.Make<PhysicalBufferedCollector>(data, false);
		}
		return physical_plan.Make<PhysicalMaterializedCollector>(data, false);
	}

	// Order-preserving plan, and we can use the batch index: use a batch collector.
	if (data.output_type == QueryResultOutputType::ALLOW_STREAMING) {
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

unique_ptr<ColumnDataCollection> PhysicalResultCollector::CreateCollection(ClientContext &context) const {
	switch (memory_type) {
	case QueryResultMemoryType::IN_MEMORY:
		return make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	case QueryResultMemoryType::BUFFER_MANAGED:
		// Use the DatabaseInstance BufferManager because the query result can outlive the ClientContext
		return make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(*context.db), types,
		                                       ColumnDataCollectionLifetime::THROW_ERROR_AFTER_DATABASE_CLOSES);
	default:
		throw NotImplementedException("PhysicalResultCollector::CreateCollection for %s",
		                              EnumUtil::ToString(memory_type));
	}
}

} // namespace duckdb
