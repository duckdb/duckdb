#include "duckdb/parallel/pipeline_event.hpp"

namespace duckdb {

PipelineEvent::PipelineEvent(shared_ptr<Pipeline> pipeline_p) :
	Event(pipeline_p->executor), pipeline(move(pipeline_p)) {}

void PipelineEvent::Schedule() {
	auto event = shared_from_this();
	pipeline->Schedule(event);
	D_ASSERT(total_tasks > 0);
}

void PipelineEvent::FinishEvent() {
	if (!pipeline->child_pipelines.empty()) {
		D_ASSERT(pipeline->child_pipelines.size() == 1);
		auto new_event = make_unique<PipelineEvent>(pipeline->child_pipelines[0]);
		InsertEvent(move(new_event));
	}
}

}
