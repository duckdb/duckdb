#include "duckdb/parallel/meta_pipeline.hpp"

#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"

namespace duckdb {

MetaPipeline::MetaPipeline(Executor &executor_p, PipelineBuildState &state_p, PhysicalOperator *sink_p)
    : executor(executor_p), state(state_p), sink(sink_p), recursive_cte(false), next_batch_index(0) {
	CreatePipeline();
}

Executor &MetaPipeline::GetExecutor() const {
	return executor;
}

PipelineBuildState &MetaPipeline::GetState() const {
	return state;
}

optional_ptr<PhysicalOperator> MetaPipeline::GetSink() const {
	return sink;
}

shared_ptr<Pipeline> &MetaPipeline::GetBasePipeline() {
	return pipelines[0];
}

void MetaPipeline::GetPipelines(vector<shared_ptr<Pipeline>> &result, bool recursive) {
	result.insert(result.end(), pipelines.begin(), pipelines.end());
	if (recursive) {
		for (auto &child : children) {
			child->GetPipelines(result, true);
		}
	}
}

void MetaPipeline::GetMetaPipelines(vector<shared_ptr<MetaPipeline>> &result, bool recursive, bool skip) {
	if (!skip) {
		result.push_back(shared_from_this());
	}
	if (recursive) {
		for (auto &child : children) {
			child->GetMetaPipelines(result, true, false);
		}
	}
}

const vector<Pipeline *> *MetaPipeline::GetDependencies(Pipeline *dependant) const {
	auto it = dependencies.find(dependant);
	if (it == dependencies.end()) {
		return nullptr;
	} else {
		return &it->second;
	}
}

bool MetaPipeline::HasRecursiveCTE() const {
	return recursive_cte;
}

void MetaPipeline::SetRecursiveCTE() {
	recursive_cte = true;
}

void MetaPipeline::AssignNextBatchIndex(Pipeline *pipeline) {
	pipeline->base_batch_index = next_batch_index++ * PipelineBuildState::BATCH_INCREMENT;
}

void MetaPipeline::Build(PhysicalOperator &op) {
	D_ASSERT(pipelines.size() == 1);
	D_ASSERT(children.empty());
	op.BuildPipelines(*pipelines.back(), *this);
}

void MetaPipeline::Ready() {
	for (auto &pipeline : pipelines) {
		pipeline->Ready();
	}
	for (auto &child : children) {
		child->Ready();
	}
}

MetaPipeline &MetaPipeline::CreateChildMetaPipeline(Pipeline &current, PhysicalOperator &op) {
	children.push_back(make_shared<MetaPipeline>(executor, state, &op));
	auto child_meta_pipeline = children.back().get();
	// child MetaPipeline must finish completely before this MetaPipeline can start
	current.AddDependency(child_meta_pipeline->GetBasePipeline());
	// child meta pipeline is part of the recursive CTE too
	child_meta_pipeline->recursive_cte = recursive_cte;
	return *child_meta_pipeline;
}

Pipeline *MetaPipeline::CreatePipeline() {
	pipelines.emplace_back(make_shared<Pipeline>(executor));
	state.SetPipelineSink(*pipelines.back(), sink, next_batch_index++);
	return pipelines.back().get();
}

void MetaPipeline::AddDependenciesFrom(Pipeline *dependant, Pipeline *start, bool including) {
	// find 'start'
	auto it = pipelines.begin();
	for (; it->get() != start; it++) {
	}

	if (!including) {
		it++;
	}

	// collect pipelines that were created from then
	vector<Pipeline *> created_pipelines;
	for (; it != pipelines.end(); it++) {
		if (it->get() == dependant) {
			// cannot depend on itself
			continue;
		}
		created_pipelines.push_back(it->get());
	}

	// add them to the dependencies
	auto &deps = dependencies[dependant];
	deps.insert(deps.begin(), created_pipelines.begin(), created_pipelines.end());
}

void MetaPipeline::AddFinishEvent(Pipeline *pipeline) {
	D_ASSERT(finish_pipelines.find(pipeline) == finish_pipelines.end());
	finish_pipelines.insert(pipeline);

	// add all pipelines that were added since 'pipeline' was added (including 'pipeline') to the finish group
	auto it = pipelines.begin();
	for (; it->get() != pipeline; it++) {
	}
	it++;
	for (; it != pipelines.end(); it++) {
		finish_map.emplace(it->get(), pipeline);
	}
}

bool MetaPipeline::HasFinishEvent(Pipeline *pipeline) const {
	return finish_pipelines.find(pipeline) != finish_pipelines.end();
}

optional_ptr<Pipeline> MetaPipeline::GetFinishGroup(Pipeline *pipeline) const {
	auto it = finish_map.find(pipeline);
	return it == finish_map.end() ? nullptr : it->second;
}

Pipeline *MetaPipeline::CreateUnionPipeline(Pipeline &current, bool order_matters) {
	// create the union pipeline (batch index 0, should be set correctly afterwards)
	auto union_pipeline = CreatePipeline();
	state.SetPipelineOperators(*union_pipeline, state.GetPipelineOperators(current));
	state.SetPipelineSink(*union_pipeline, sink, 0);

	// 'union_pipeline' inherits ALL dependencies of 'current' (within this MetaPipeline, and across MetaPipelines)
	union_pipeline->dependencies = current.dependencies;
	auto current_deps = GetDependencies(&current);
	if (current_deps) {
		dependencies[union_pipeline] = *current_deps;
	}

	if (order_matters) {
		// if we need to preserve order, or if the sink is not parallel, we set a dependency
		dependencies[union_pipeline].push_back(&current);
	}

	return union_pipeline;
}

void MetaPipeline::CreateChildPipeline(Pipeline &current, PhysicalOperator &op, Pipeline *last_pipeline) {
	// rule 2: 'current' must be fully built (down to the source) before creating the child pipeline
	D_ASSERT(current.source);

	// create the child pipeline (same batch index)
	pipelines.emplace_back(state.CreateChildPipeline(executor, current, op));
	auto child_pipeline = pipelines.back().get();
	child_pipeline->base_batch_index = current.base_batch_index;

	// child pipeline has a dependency (within this MetaPipeline on all pipelines that were scheduled
	// between 'current' and now (including 'current') - set them up
	dependencies[child_pipeline].push_back(&current);
	AddDependenciesFrom(child_pipeline, last_pipeline, false);
	D_ASSERT(!GetDependencies(child_pipeline)->empty());
}

} // namespace duckdb
