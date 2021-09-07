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
	// check if there are more sources to schedule within this pipeline
	for(idx_t i = pipeline->source_idx + 1; i < pipeline->operators.size(); i++) {
		if (pipeline->operators[i]->IsSource()) {
			// there is another source! schedule it instead of finishing this event
			pipeline->source_idx = i;
			pipeline->ResetSource();
			auto new_event = make_unique<PipelineEvent>(pipeline);
			InsertEvent(move(new_event));
			return;
		}
	}
}

}
