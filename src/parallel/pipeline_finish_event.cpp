#include "duckdb/parallel/pipeline_finish_event.hpp"
#include "duckdb/execution/executor.hpp"

namespace duckdb {

PipelineFinishEvent::PipelineFinishEvent(shared_ptr<Pipeline> pipeline_p) : BasePipelineEvent(Move(pipeline_p)) {
}

void PipelineFinishEvent::Schedule() {
}

void PipelineFinishEvent::FinishEvent() {
	pipeline->Finalize(*this);
}

} // namespace duckdb
