#include "duckdb/execution/operator/set/physical_union.hpp"

#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

PhysicalUnion::PhysicalUnion(vector<LogicalType> types, unique_ptr<PhysicalOperator> top,
                             unique_ptr<PhysicalOperator> bottom, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::UNION, move(types), estimated_cardinality) {
	children.push_back(move(top));
	children.push_back(move(bottom));
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalUnion::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	sink_state.reset();

	// create a union pipeline that is identical to 'current'
	auto union_pipeline = meta_pipeline.CreateUnionPipeline(current);

	// continue with the current pipeline
	children[0]->BuildPipelines(current, meta_pipeline);

	auto sink = meta_pipeline.GetSink();
	if (sink && sink->IsOrderPreserving() && !sink->RequiresBatchIndex()) {
		// we want to preserve order but we can't use batch indices, therefore
		// 'union_pipeline' must come after all pipelines created by building out 'current'
		meta_pipeline.AddDependenciesFrom(union_pipeline, union_pipeline, false);
	}

	// build the union pipeline
	children[1]->BuildPipelines(*union_pipeline, meta_pipeline);

	// Assign proper batch index to the union pipeline
	// This needs to happen after the pipelines have been built because unions can be nested
	meta_pipeline.AssignNextBatchIndex(union_pipeline);
}

vector<const PhysicalOperator *> PhysicalUnion::GetSources() const {
	vector<const PhysicalOperator *> result;
	for (auto &child : children) {
		auto child_sources = child->GetSources();
		result.insert(result.end(), child_sources.begin(), child_sources.end());
	}
	return result;
}

} // namespace duckdb
