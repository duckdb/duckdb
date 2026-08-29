#include "duckdb/parallel/pipeline_result_ready_event.hpp"

#include "duckdb/execution/executor.hpp"

namespace duckdb {

PipelineResultReadyEvent::PipelineResultReadyEvent(Executor &executor_p)
    : Event(executor_p), state(PipelineResultReadyEventState::WAITING) {
}

void PipelineResultReadyEvent::Schedule() {
	auto expected = PipelineResultReadyEventState::WAITING;
	if (!state.compare_exchange_strong(expected, PipelineResultReadyEventState::READY)) {
		throw InternalException("PipelineResultReadyEvent scheduled more than once");
	}
	executor.NotifyResultReady();
}

bool PipelineResultReadyEvent::AutoFinishWithoutTasks() const {
	return false;
}

void PipelineResultReadyEvent::Open() {
	auto expected = PipelineResultReadyEventState::READY;
	if (!state.compare_exchange_strong(expected, PipelineResultReadyEventState::OPENED)) {
		throw InternalException("PipelineResultReadyEvent opened before it was ready or more than once");
	}
	Finish();
}

} // namespace duckdb
