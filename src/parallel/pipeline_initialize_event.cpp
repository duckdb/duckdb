#include "duckdb/parallel/pipeline_initialize_event.hpp"
#include "duckdb/execution/executor.hpp"

namespace duckdb {

PipelineInitializeEvent::PipelineInitializeEvent(shared_ptr<Pipeline> pipeline_p) : BasePipelineEvent(move(pipeline_p)) {
}

void PipelineInitializeEvent::Schedule() {
	pipeline->Reset();
}

void PipelineInitializeEvent::FinishEvent() {
}

} // namespace duckdb
