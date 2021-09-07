#include "duckdb/parallel/pipeline_finish_event.hpp"
#include "duckdb/execution/executor.hpp"

namespace duckdb {

PipelineFinishEvent::PipelineFinishEvent(const shared_ptr<Pipeline> &pipeline_p) :
	Event(pipeline_p->executor), pipeline_w(weak_ptr<Pipeline>(pipeline_p)) {}

void PipelineFinishEvent::Schedule() {
	Finish();
}

void PipelineFinishEvent::FinishEvent() {
	auto pipeline = pipeline_w.lock();
	if (!pipeline) {
		return;
	}
	pipeline->Finalize(*this);
}

}
