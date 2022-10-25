#include "duckdb/execution/operator/set/physical_union.hpp"

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
void PhysicalUnion::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline,
                                   vector<Pipeline *> &final_pipelines) {
	op_state.reset();
	sink_state.reset();

	// create a union pipeline that is identical
	auto union_pipeline = meta_pipeline.CreateUnionPipeline(current);
	// continue building the current pipeline
	children[0]->BuildPipelines(current, meta_pipeline, final_pipelines);
	// continue building the union pipeline
	children[1]->BuildPipelines(*union_pipeline, meta_pipeline, final_pipelines);
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
