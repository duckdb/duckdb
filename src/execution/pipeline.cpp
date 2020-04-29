#include "duckdb/execution/pipeline.hpp"
#include "duckdb/execution/execution_context.hpp"

namespace duckdb {

Pipeline::Pipeline(ExecutionContext &execution_context) :
	execution_context(execution_context), parent(nullptr) {

}

void Pipeline::Execute(ClientContext &context) {
	assert(dependents.size() == 0);
	auto state = child->GetOperatorState();
	auto lstate = sink->GetLocalSinkState(context);
	// incrementally process the pipeline
	DataChunk intermediate;
	child->InitializeChunk(intermediate);
	while(true) {
		child->GetChunk(context, intermediate, state.get());
		if (intermediate.size() == 0) {
			sink->Finalize(context, *sink_state, *lstate);
			break;
		}
		sink->Sink(context, *sink_state, *lstate, intermediate);
	}
	Finish();
}

void Pipeline::EraseDependent(Pipeline *pipeline) {
	assert(std::find_if(dependents.begin(), dependents.end(), [&](std::unique_ptr<Pipeline>& p) {
		return p.get() == pipeline;
	}) != dependents.end());

	dependents.erase(std::find_if(dependents.begin(), dependents.end(), [&](std::unique_ptr<Pipeline>& p) {
		return p.get() == pipeline;
	}));
	if (dependents.size() == 0) {
		// no more dependents: schedule this pipeline
		execution_context.Schedule(this);
	}
}

void Pipeline::Finish() {
	sink->sink_state = move(sink_state);

	// finished processing the pipeline, now we can schedule pipelines that depend on this pipeline
	if (parent) {
		// parent: remove this entry from the dependents
		parent->EraseDependent(this);
	} else {
		// no parent: erase pipeline from the execution context
		execution_context.EraseDependent(this);
	}
}

}
