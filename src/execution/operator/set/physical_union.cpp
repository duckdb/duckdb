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

	// create a union pipeline that is identical, inheriting any dependencies encountered so far
	auto union_pipeline = meta_pipeline.CreateUnionPipeline(current);

	// continue with the current pipeline
	children[0]->BuildPipelines(current, meta_pipeline);

	if (meta_pipeline.PreservesOrder()) {
		// 'union_pipeline' must come after all pipelines created by building out 'current' if we want to preserve order
		meta_pipeline.AddDependenciesFrom(union_pipeline, union_pipeline);
		// FIXME: use batch index to parallelize/preserve order
	}

	// build the union pipeline
	children[1]->BuildPipelines(*union_pipeline, meta_pipeline);
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
