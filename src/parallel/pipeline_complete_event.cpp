#include "duckdb/parallel/pipeline_complete_event.hpp"
#include "duckdb/execution/executor.hpp"

namespace duckdb {

PipelineCompleteEvent::PipelineCompleteEvent(Executor &executor, bool complete_pipeline_p)
    : Event(executor), complete_pipeline(complete_pipeline_p) {
}

void PipelineCompleteEvent::Schedule() {
}

void PipelineCompleteEvent::FinalizeFinish() {
	if (complete_pipeline) {
		executor.CompletePipeline();
	}
}

} // namespace duckdb
