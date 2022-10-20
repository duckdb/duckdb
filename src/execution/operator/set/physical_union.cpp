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
void PhysicalUnion::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	if (state.recursive_cte) {
		throw NotImplementedException("UNIONS are not supported in recursive CTEs yet");
	}
	op_state.reset();
	sink_state.reset();

	auto union_pipeline = make_shared<Pipeline>(executor);
	auto pipeline_ptr = union_pipeline.get();

	// child pipeline dependencies are in reverse order of scheduling: for 'current', dependencies are set up correctly
	// however, the union pipeline may also have child pipelines
	// all child pipelines of 'current' that are scheduled before going further down the pipeline
	// depend on the child pipelines of the union pipeline being finished too
	auto &child_pipelines = state.GetChildPipelines(executor);
	vector<shared_ptr<Pipeline>> child_pipelines_before;
	auto it = child_pipelines.find(&current);
	if (it != child_pipelines.end()) {
		child_pipelines_before = it->second;
	}

	auto &union_pipelines = state.GetUnionPipelines(executor);
	// for the current pipeline, continue building on the LHS
	state.SetPipelineOperators(*union_pipeline, state.GetPipelineOperators(current));
	children[0]->BuildPipelines(executor, current, state);
	// insert the union pipeline as a union pipeline of the current node
	union_pipelines[&current].push_back(move(union_pipeline));

	// for the union pipeline, build on the RHS
	state.SetPipelineSink(*pipeline_ptr, state.GetPipelineSink(current));
	children[1]->BuildPipelines(executor, *pipeline_ptr, state);

	if (child_pipelines_before.empty()) {
		// no child pipelines, no need to set up dependencies
		return;
	}

	// as stated above, child_pipelines_before must depend on the child pipelines of the union pipeline
	// also as stated above, child pipelines are dependent of each other in reverse order
	// therefore it suffices to make child_pipelines_before[0] dependent on the 1st child pipeline of the union pipeline
	it = child_pipelines.find(pipeline_ptr);
	if (it != child_pipelines.end()) {
		auto &union_child_pipelines = it->second;
		//		union_child_pipelines[0]->AddDependency(child_pipelines_before[0]);
//		child_pipelines_before[0]->AddDependency(union_child_pipelines[0]);
	}
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
