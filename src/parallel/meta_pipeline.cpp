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

shared_ptr<Pipeline> &MetaPipeline::GetBasePipeline() {
	return pipelines[0];
}

void MetaPipeline::GetPipelines(vector<shared_ptr<Pipeline>> &result, bool recursive, bool skip) {
	if (!skip) {
		result.insert(result.end(), pipelines.begin(), pipelines.end());
	}
	if (recursive) {
		for (auto &child : children) {
			child->GetPipelines(result, true, false);
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
	return recursive_cte != nullptr;
}

PhysicalRecursiveCTE *MetaPipeline::GetRecursiveCTE() const {
	return (PhysicalRecursiveCTE *)recursive_cte;
}

void MetaPipeline::SetRecursiveCTE(PhysicalOperator *recursive_cte_p) {
	D_ASSERT(recursive_cte_p->type == PhysicalOperatorType::RECURSIVE_CTE);
	recursive_cte = (PhysicalRecursiveCTE *)recursive_cte_p;
}

void MetaPipeline::Build(PhysicalOperator *op) {
	D_ASSERT(pipelines.size() == 1);
	D_ASSERT(children.empty());
	D_ASSERT(final_pipelines.empty());
	op->BuildPipelines(*pipelines.back(), *this);
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
	D_ASSERT(pipelines.back().get() == &current); // rule 1
	children.push_back(make_unique<MetaPipeline>(executor, state, op));
	auto child_meta_pipeline = children.back().get();
	// child MetaPipeline must finish completely before this MetaPipeline can start
	current.AddDependency(child_meta_pipeline->GetBasePipeline());
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

	// create the union pipeline
	auto union_pipeline = CreatePipeline();
	state.SetPipelineOperators(*union_pipeline, state.GetPipelineOperators(current));
	state.SetPipelineSink(*union_pipeline, sink, pipelines.size() - 1);

	// 'union_pipeline' inherits ALL dependencies of 'current' (intra- and inter-MetaPipeline)
	union_pipeline->dependencies = current.dependencies;
	auto current_inter_deps = GetDependencies(&current);
	if (current_inter_deps) {
		dependencies[union_pipeline] = *current_inter_deps;
	}

	return union_pipeline;
}

void MetaPipeline::CreateChildPipeline(Pipeline &current, PhysicalOperator *op) {
	D_ASSERT(pipelines.back().get() != &current); // rule 2
	if (HasRecursiveCTE()) {
		throw NotImplementedException("Child pipelines are not supported in recursive CTEs yet");
	}

	// child pipeline has an inter-MetaPipeline depency on all pipelines that were scheduled between 'current' and now
	// (including 'current') - gather them
	auto it = pipelines.begin();
	while (it->get() != &current) {
		it++;
	}
	vector<Pipeline *> scheduled_between;
	while (it != pipelines.end()) {
		scheduled_between.push_back(it->get());
	}
	D_ASSERT(!scheduled_between.empty());

	// finally, create the child pipeline and set the dependencies
	pipelines.emplace_back(state.CreateChildPipeline(executor, current, op));
	dependencies[pipelines.back().get()] = move(scheduled_between);
}

} // namespace duckdb
