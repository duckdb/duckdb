#include "duckdb/parallel/pipeline_complete_event.hpp"
#include "duckdb/execution/executor.hpp"

namespace duckdb {

PipelineCompleteEvent::PipelineCompleteEvent(Executor &executor) :
	Event(executor), total_pipelines(1) {}

void PipelineCompleteEvent::Schedule() {
	Finish();
}

void PipelineCompleteEvent::FinalizeFinish() {
	for(idx_t i = 0; i < total_pipelines; i++) {
		executor.CompletePipeline();
	}
}

}
