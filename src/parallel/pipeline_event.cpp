#include "duckdb/parallel/pipeline_event.hpp"

namespace duckdb {

PipelineEvent::PipelineEvent(shared_ptr<Pipeline> pipeline_p) :
	Event(pipeline_p->executor), pipeline(move(pipeline_p)) {}

void PipelineEvent::Schedule() {
	auto event = shared_from_this();
	pipeline->Schedule(event);
}

void PipelineEvent::FinishEvent() {
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
