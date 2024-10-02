#include "duckdb/parallel/meta_pipeline.hpp"

#include "duckdb/execution/executor.hpp"

namespace duckdb {

MetaPipeline::MetaPipeline(Executor &executor_p, PipelineBuildState &state_p, optional_ptr<PhysicalOperator> sink_p,
                           MetaPipelineType type_p)
    : executor(executor_p), state(state_p), sink(sink_p), type(type_p), recursive_cte(false), next_batch_index(0) {
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

optional_ptr<Pipeline> MetaPipeline::GetParent() const {
	return parent;
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
	for (auto &child : children) {
		result.push_back(child);
		if (recursive) {
			child->GetMetaPipelines(result, true, true);
		}
	}
}

MetaPipeline &MetaPipeline::GetLastChild() {
	if (children.empty()) {
		return *this;
	}
	reference<const vector<shared_ptr<MetaPipeline>>> current_children = children;
	while (!current_children.get().back()->children.empty()) {
		current_children = current_children.get().back()->children;
	}
	return *current_children.get().back();
}

const reference_map_t<Pipeline, vector<reference<Pipeline>>> &MetaPipeline::GetDependencies() const {
	return pipeline_dependencies;
}

MetaPipelineType MetaPipeline::Type() const {
	return type;
}

bool MetaPipeline::HasRecursiveCTE() const {
	return recursive_cte;
}

void MetaPipeline::SetRecursiveCTE() {
	recursive_cte = true;
}

void MetaPipeline::AssignNextBatchIndex(Pipeline &pipeline) {
	pipeline.base_batch_index = next_batch_index++ * PipelineBuildState::BATCH_INCREMENT;
}

void MetaPipeline::Build(PhysicalOperator &op) {
	D_ASSERT(pipelines.size() == 1);
	D_ASSERT(children.empty());
	op.BuildPipelines(*pipelines.back(), *this);
}

void MetaPipeline::Ready() const {
	for (auto &pipeline : pipelines) {
		pipeline->Ready();
	}
	for (auto &child : children) {
		child->Ready();
	}
}

MetaPipeline &MetaPipeline::CreateChildMetaPipeline(Pipeline &current, PhysicalOperator &op, MetaPipelineType type) {
	children.push_back(make_shared_ptr<MetaPipeline>(executor, state, &op, type));
	auto &child_meta_pipeline = *children.back().get();
	// store the parent
	child_meta_pipeline.parent = &current;
	// child MetaPipeline must finish completely before this MetaPipeline can start
	current.AddDependency(child_meta_pipeline.GetBasePipeline());
	// child meta pipeline is part of the recursive CTE too
	child_meta_pipeline.recursive_cte = recursive_cte;
	return child_meta_pipeline;
}

Pipeline &MetaPipeline::CreatePipeline() {
	pipelines.emplace_back(make_shared_ptr<Pipeline>(executor));
	state.SetPipelineSink(*pipelines.back(), sink, next_batch_index++);
	return *pipelines.back();
}

vector<shared_ptr<Pipeline>> MetaPipeline::AddDependenciesFrom(Pipeline &dependant, const Pipeline &start,
                                                               const bool including) {
	// find 'start'
	auto it = pipelines.begin();
	for (; !RefersToSameObject(**it, start); it++) {
	}

	if (!including) {
		it++;
	}

	// collect pipelines that were created from then
	vector<shared_ptr<Pipeline>> created_pipelines;
	for (; it != pipelines.end(); it++) {
		if (RefersToSameObject(**it, dependant)) {
			// cannot depend on itself
			continue;
		}
		created_pipelines.push_back(*it);
	}

	// add them to the dependencies
	auto &explicit_deps = pipeline_dependencies[dependant];
	for (auto &created_pipeline : created_pipelines) {
		explicit_deps.push_back(*created_pipeline);
	}

	return created_pipelines;
}

static bool PipelineExceedsThreadCount(Pipeline &pipeline, const idx_t thread_count) {
#ifdef DEBUG
	// we always add the dependency in debug mode so that this is well-tested
	return true;
#else
	return pipeline.GetSource()->EstimatedThreadCount() > thread_count;
#endif
}

void MetaPipeline::AddRecursiveDependencies(const vector<shared_ptr<Pipeline>> &new_dependencies,
                                            const MetaPipeline &last_child) {
	if (recursive_cte) {
		return; // let's not burn our fingers on this for now
	}

	vector<shared_ptr<MetaPipeline>> child_meta_pipelines;
	this->GetMetaPipelines(child_meta_pipelines, true, false);

	// find the meta pipeline that has the same sink as 'pipeline'
	auto it = child_meta_pipelines.begin();
	for (; !RefersToSameObject(last_child, **it); it++) {
	}
	D_ASSERT(it != child_meta_pipelines.end());

	// skip over it
	it++;

	// we try to limit the performance impact of these dependencies on smaller workloads,
	// by only adding the dependencies if the source operator can likely keep all threads busy
	const auto thread_count = NumericCast<idx_t>(TaskScheduler::GetScheduler(executor.context).NumberOfThreads());
	for (; it != child_meta_pipelines.end(); it++) {
		for (auto &pipeline : it->get()->pipelines) {
			if (!PipelineExceedsThreadCount(*pipeline, thread_count)) {
				continue;
			}
			auto &pipeline_deps = pipeline_dependencies[*pipeline];
			for (auto &new_dependency : new_dependencies) {
				if (!PipelineExceedsThreadCount(*new_dependency, thread_count)) {
					continue;
				}
				pipeline_deps.push_back(*new_dependency);
			}
		}
	}
}

void MetaPipeline::AddFinishEvent(Pipeline &pipeline) {
	D_ASSERT(finish_pipelines.find(pipeline) == finish_pipelines.end());
	finish_pipelines.insert(pipeline);

	// add all pipelines that were added since 'pipeline' was added (including 'pipeline') to the finish group
	auto it = pipelines.begin();
	for (; !RefersToSameObject(**it, pipeline); it++) {
	}
	it++;
	for (; it != pipelines.end(); it++) {
		finish_map.emplace(**it, pipeline);
	}
}

bool MetaPipeline::HasFinishEvent(Pipeline &pipeline) const {
	return finish_pipelines.find(pipeline) != finish_pipelines.end();
}

optional_ptr<Pipeline> MetaPipeline::GetFinishGroup(Pipeline &pipeline) const {
	auto it = finish_map.find(pipeline);
	return it == finish_map.end() ? nullptr : &it->second;
}

Pipeline &MetaPipeline::CreateUnionPipeline(Pipeline &current, bool order_matters) {
	// create the union pipeline (batch index 0, should be set correctly afterwards)
	auto &union_pipeline = CreatePipeline();
	state.SetPipelineOperators(union_pipeline, state.GetPipelineOperators(current));
	state.SetPipelineSink(union_pipeline, sink, 0);

	// 'union_pipeline' inherits ALL dependencies of 'current' (within this MetaPipeline, and across MetaPipelines)
	union_pipeline.dependencies = current.dependencies;
	auto it = pipeline_dependencies.find(current);
	if (it != pipeline_dependencies.end()) {
		pipeline_dependencies[union_pipeline] = it->second;
	}

	if (order_matters) {
		// if we need to preserve order, or if the sink is not parallel, we set a dependency
		pipeline_dependencies[union_pipeline].push_back(current);
	}

	return union_pipeline;
}

void MetaPipeline::CreateChildPipeline(Pipeline &current, PhysicalOperator &op, Pipeline &last_pipeline) {
	// rule 2: 'current' must be fully built (down to the source) before creating the child pipeline
	D_ASSERT(current.source);

	// create the child pipeline (same batch index)
	pipelines.emplace_back(state.CreateChildPipeline(executor, current, op));
	auto &child_pipeline = *pipelines.back();
	child_pipeline.base_batch_index = current.base_batch_index;

	// child pipeline has a dependency (within this MetaPipeline on all pipelines that were scheduled
	// between 'current' and now (including 'current') - set them up
	pipeline_dependencies[child_pipeline].push_back(current);
	AddDependenciesFrom(child_pipeline, last_pipeline, false);
	D_ASSERT(pipeline_dependencies.find(child_pipeline) != pipeline_dependencies.end());
}

} // namespace duckdb
