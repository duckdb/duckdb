#include "duckdb/execution/operator/helper/physical_result_collector.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/operator/helper/physical_result_sink.hpp"
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

unique_ptr<PhysicalOperator> PhysicalResultCollector::GetResultCollector(ClientContext &context,
                                                                         PreparedStatementData &data) {
	auto &physical_plan = *data.physical_plan;
	auto &root = physical_plan.Root();

	const auto lifetime = data.output_type == QueryResultOutputType::ALLOW_STREAMING ? ResultLifetime::UNDECIDED
	                                                                                 : ResultLifetime::RETAINED;
	if (!PhysicalPlanGenerator::PreserveInsertionOrder(context, root)) {
		return make_uniq<PhysicalResultSink>(physical_plan, data, lifetime, ResultOrdering::UNORDERED);
	}
	if (!PhysicalPlanGenerator::UseBatchIndex(context, root)) {
		return make_uniq<PhysicalResultSink>(physical_plan, data, lifetime, ResultOrdering::SOURCE_ORDERED);
	}
	return make_uniq<PhysicalResultSink>(physical_plan, data, lifetime, ResultOrdering::BATCH_INDEX_ORDERED);
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
