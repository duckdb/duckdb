#include "duckdb/execution/pipeline.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

Pipeline::Pipeline(ExecutionContext &execution_context) :
	execution_context(execution_context) {

}

void Pipeline::Execute(ClientContext &context) {
	assert(dependencies.size() == 0);
	auto state = child->GetOperatorState();
	auto lstate = sink->GetLocalSinkState(context);
	// incrementally process the pipeline
	DataChunk intermediate;
	child->InitializeChunk(intermediate);
	while(true) {
		child->GetChunk(context, intermediate, state.get());
		if (intermediate.size() == 0) {
			sink->Combine(context, *sink_state, *lstate);
			sink->Finalize(context, move(sink_state));
			break;
		}
		sink->Sink(context, *sink_state, *lstate, intermediate);
	}
	Finish();
}

void Pipeline::AddDependency(Pipeline *pipeline) {
	this->dependencies.insert(pipeline);
	pipeline->parents.insert(this);
}

void Pipeline::EraseDependency(Pipeline *pipeline) {
	assert(dependencies.count(pipeline) == 1);

	dependencies.erase(dependencies.find(pipeline));
	if (dependencies.size() == 0) {
		// no more dependents: schedule this pipeline
		execution_context.Schedule(this);
	}
}

void Pipeline::Finish() {
	// finished processing the pipeline, now we can schedule pipelines that depend on this pipeline
	for(auto &parent : parents) {
		// parent: remove this entry from the dependents
		parent->EraseDependency(this);
	}
	// erase pipeline from the execution context
	execution_context.ErasePipeline(this);
}

string Pipeline::ToString() const {
	string str = PhysicalOperatorToString(sink->type);
	auto node = this->child;
	while(node) {
		str = PhysicalOperatorToString(node->type) + " -> " + str;
		node = node->children[0].get();
	}
	return str;
}

void Pipeline::Print() const {
	Printer::Print(ToString());
}

}
