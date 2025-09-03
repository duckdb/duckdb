#include "duckdb/parallel/pipeline_event.hpp"
#include "duckdb/execution/executor.hpp"

namespace duckdb {

PipelineEvent::PipelineEvent(shared_ptr<Pipeline> pipeline_p) : BasePipelineEvent(std::move(pipeline_p)) {
}

void PipelineEvent::Schedule() {
	auto event = shared_from_this();
	auto &executor = pipeline->executor;
	try {
		pipeline->Schedule(event);
		D_ASSERT(total_tasks > 0);
	} catch (std::exception &ex) {
		executor.PushError(ErrorData(ex));
	} catch (...) { // LCOV_EXCL_START
		executor.PushError(ErrorData("Unknown exception while calling pipeline->Schedule(event)!"));
	} // LCOV_EXCL_STOP
}

void PipelineEvent::FinishEvent() {
}

} // namespace duckdb
