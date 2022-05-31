#include "duckdb/execution/operator/set/physical_union.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/pipeline.hpp"

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
void PhysicalUnion::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	if (state.recursive_cte) {
		throw NotImplementedException("UNIONS are not supported in recursive CTEs yet");
	}
	op_state.reset();
	sink_state.reset();

	auto union_pipeline = make_shared<Pipeline>(executor);
	auto pipeline_ptr = union_pipeline.get();
	auto &child_pipelines = state.GetChildPipelines(executor);
	auto &child_dependencies = state.GetChildDependencies(executor);
	auto &union_pipelines = state.GetUnionPipelines(executor);
	// set up dependencies for any child pipelines to this union pipeline
	auto child_entry = child_pipelines.find(&current);
	if (child_entry != child_pipelines.end()) {
		for (auto &current_child : child_entry->second) {
			D_ASSERT(child_dependencies.find(current_child.get()) != child_dependencies.end());
			child_dependencies[current_child.get()].push_back(pipeline_ptr);
		}
	}
	// for the current pipeline, continue building on the LHS
	state.SetPipelineOperators(*union_pipeline, state.GetPipelineOperators(current));
	children[0]->BuildPipelines(executor, current, state);
	// insert the union pipeline as a union pipeline of the current node
	union_pipelines[&current].push_back(move(union_pipeline));

	// for the union pipeline, build on the RHS
	state.SetPipelineSink(*pipeline_ptr, state.GetPipelineSink(current));
	children[1]->BuildPipelines(executor, *pipeline_ptr, state);
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
