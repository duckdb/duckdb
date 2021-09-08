#include "duckdb/parallel/pipeline_event.hpp"

namespace duckdb {

PipelineEvent::PipelineEvent(const shared_ptr<Pipeline> &pipeline_p) :
	Event(pipeline_p->executor), pipeline_w(weak_ptr<Pipeline>(pipeline_p)) {}

void PipelineEvent::Schedule() {
	auto event = shared_from_this();
	auto pipeline = pipeline_w.lock();
	if (!pipeline) {
		return;
	}
	pipeline->Schedule(event);
}

void PipelineEvent::FinishEvent() {
	auto pipeline = pipeline_w.lock();
	if (!pipeline) {
		return;
	}
	if (pipeline->NextSource()) {
		auto new_event = make_unique<PipelineEvent>(pipeline);
		InsertEvent(move(new_event));
	}
}

}
