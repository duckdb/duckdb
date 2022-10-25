#include "duckdb/parallel/meta_pipeline.hpp"

#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"

namespace duckdb {

MetaPipeline::MetaPipeline(Executor &executor_p, PipelineBuildState &state_p, PhysicalOperator *sink_p)
    : executor(executor_p), state(state_p), sink(sink_p) {
	auto root_pipeline = CreatePipeline();
	state.SetPipelineSink(*root_pipeline, sink, 0);
}

Executor &MetaPipeline::GetExecutor() const {
	return executor;
}

PipelineBuildState &MetaPipeline::GetState() const {
	return state;
}

PhysicalOperator *MetaPipeline::GetSink() const {
	return sink;
}

bool MetaPipeline::HasRecursiveCTE() const {
	return recursive_cte != nullptr;
}

PhysicalRecursiveCTE *MetaPipeline::GetRecursiveCTE() const {
	return (PhysicalRecursiveCTE *)recursive_cte;
}

void MetaPipeline::SetRecursiveCTE(PhysicalOperator *recursive_cte_p) {
	D_ASSERT(recursive_cte_p->type == PhysicalOperatorType::RECURSIVE_CTE);
	recursive_cte = (PhysicalRecursiveCTE *)recursive_cte_p;
}

shared_ptr<Pipeline> &MetaPipeline::GetRootPipeline() {
	return pipelines[0];
}

void MetaPipeline::GetPipelines(vector<shared_ptr<Pipeline>> &result, bool recursive) {
	if (!sink) {
		return;
	}
	result.insert(result.end(), pipelines.begin(), pipelines.end());
	if (recursive) {
		for (auto &child : children) {
			child->GetPipelines(result, true);
		}
	}
}

void MetaPipeline::GetMetaPipelines(vector<shared_ptr<MetaPipeline>> &result, bool recursive) {
	result.push_back(shared_from_this());
	if (recursive) {
		for (auto &child : children) {
			child->GetMetaPipelines(result, true);
		}
	}
}

vector<shared_ptr<MetaPipeline>> &MetaPipeline::GetChildren() {
	return children;
}

const vector<Pipeline *> *MetaPipeline::GetDependencies(Pipeline *pipeline) const {
	auto it = inter_pipeline_dependencies.find(pipeline);
	if (it == inter_pipeline_dependencies.end()) {
		return nullptr;
	} else {
		return &it->second;
	}
}

vector<Pipeline *> &MetaPipeline::GetFinalPipelines() {
	return final_pipelines;
}

void MetaPipeline::Build(PhysicalOperator *op) {
	D_ASSERT(pipelines.size() == 1);
	D_ASSERT(children.empty());
	D_ASSERT(final_pipelines.empty());
	op->BuildPipelines(*pipelines.back(), *this, final_pipelines);
}

void MetaPipeline::Ready() {
	for (auto &pipeline : pipelines) {
		pipeline->Ready();
	}
	for (auto &child : children) {
		child->Ready();
	}
}

void MetaPipeline::Reset(ClientContext &context, bool reset_sink) {
	if (sink && reset_sink) {
		D_ASSERT(!HasRecursiveCTE());
		sink->sink_state = sink->GetGlobalSinkState(context);
	}
	for (auto &pipeline : pipelines) {
		for (auto &op : pipeline->GetOperators()) {
			op->op_state = op->GetGlobalOperatorState(context);
		}
		pipeline->Reset();
	}
	for (auto &child : children) {
		child->Reset(context, true);
	}
}

MetaPipeline *MetaPipeline::CreateChildMetaPipeline(Pipeline &current, PhysicalOperator *op) {
	children.push_back(make_unique<MetaPipeline>(executor, state, op));
	auto child_meta_pipeline = children.back().get();
	// child MetaPipeline must finish completely before this MetaPipeline can start
	current.AddDependency(child_meta_pipeline->GetRootPipeline());
	return child_meta_pipeline;
}

Pipeline *MetaPipeline::CreatePipeline() {
	pipelines.emplace_back(make_unique<Pipeline>(executor));
	return pipelines.back().get();
}

Pipeline *MetaPipeline::CreateUnionPipeline(Pipeline &current) {
	if (HasRecursiveCTE()) {
		throw NotImplementedException("UNIONS are not supported in recursive CTEs yet");
	}
	auto union_pipeline = CreatePipeline();
	state.SetPipelineOperators(*union_pipeline, state.GetPipelineOperators(current));
	state.SetPipelineSink(*union_pipeline, sink, pipelines.size() - 1);
	return union_pipeline;
}

Pipeline *MetaPipeline::CreateChildPipeline(Pipeline &current) {
	pipelines.emplace_back(state.CreateChildPipeline(executor, current));
	return pipelines.back().get();
}

void MetaPipeline::AddInterPipelineDependency(Pipeline *dependant, Pipeline *dependee) {
	inter_pipeline_dependencies[dependant].push_back(dependee);
}

} // namespace duckdb