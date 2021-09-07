#include "duckdb/parallel/pipeline_complete_event.hpp"
#include "duckdb/execution/executor.hpp"

namespace duckdb {

PipelineCompleteEvent::PipelineCompleteEvent(Executor &executor) :
	Event(executor) {}

void PipelineCompleteEvent::Schedule() {
	Finish();
}

void PipelineCompleteEvent::FinalizeFinish() {
	executor.CompletePipeline();
}

}
