#include "duckdb/execution/operator/helper/physical_result_collector.hpp"

#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"
#include "duckdb/execution/operator/helper/physical_materialized_collector.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

PhysicalResultCollector::PhysicalResultCollector(PreparedStatementData &data)
    : PhysicalOperator(PhysicalOperatorType::RESULT_COLLECTOR, {LogicalType::BOOLEAN}, 0),
      statement_type(data.statement_type), properties(data.properties), plan(data.plan.get()), names(data.names) {
	this->types = data.types;
}

unique_ptr<PhysicalResultCollector> PhysicalResultCollector::GetResultCollector(ClientContext &context,
                                                                                PreparedStatementData &data) {
	auto &config = DBConfig::GetConfig(context);
	bool use_materialized_collector =
	    !config.options.preserve_insertion_order || !data.plan->AllSourcesSupportBatchIndex();
	if (!data.plan->AllOperatorsPreserveOrder()) {
		// the plan is not order preserving, so we just use the parallel materialized collector
		return make_unique_base<PhysicalResultCollector, PhysicalMaterializedCollector>(data, true);
	} else if (use_materialized_collector) {
		// parallel materialized collector only if we don't care about maintaining insertion order
		return make_unique_base<PhysicalResultCollector, PhysicalMaterializedCollector>(
		    data, !config.options.preserve_insertion_order);
	} else {
		// we care about maintaining insertion order and the sources all support batch indexes
		// use a batch collector
		return make_unique_base<PhysicalResultCollector, PhysicalBatchCollector>(data);
	}
}

vector<PhysicalOperator *> PhysicalResultCollector::GetChildren() const {
	return {plan};
}

void PhysicalResultCollector::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	// operator is a sink, build a pipeline
	sink_state.reset();

	D_ASSERT(children.empty());
	D_ASSERT(plan);

	// single operator: the operator becomes the data source of the current pipeline
	auto &state = meta_pipeline.GetState();
	state.SetPipelineSource(current, this);

	// we create a new pipeline starting from the child
	auto child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, this);
	child_meta_pipeline->Build(plan);
}

} // namespace duckdb
