#include "duckdb/parallel/pipeline_event.hpp"

namespace duckdb {

PipelineEvent::PipelineEvent(shared_ptr<Pipeline> pipeline_p) :
	Event(pipeline_p->executor), pipeline(move(pipeline_p)) {}

void PipelineEvent::Schedule() {
	auto event = shared_from_this();
	pipeline->Schedule(event);
}

void PipelineEvent::FinishEvent() {
	if (pipeline->NextSource()) {
		auto new_event = make_unique<PipelineEvent>(pipeline);
		InsertEvent(move(new_event));
	}
}

}
