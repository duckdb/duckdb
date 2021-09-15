#include "duckdb/parallel/pipeline_complete_event.hpp"
#include "duckdb/execution/executor.hpp"

namespace duckdb {

PipelineCompleteEvent::PipelineCompleteEvent(Executor &executor) :
	Event(executor) {}

void PipelineCompleteEvent::Schedule() {
}

void PipelineCompleteEvent::FinalizeFinish() {
	executor.CompletePipeline();
}

}
