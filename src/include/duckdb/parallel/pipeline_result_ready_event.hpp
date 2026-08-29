//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline_result_ready_event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/event.hpp"

namespace duckdb {

enum class PipelineResultReadyEventState : uint8_t { WAITING, READY, OPENED };

//! Gates the pipelines that produce a streaming query result until the result has been created.
class PipelineResultReadyEvent : public Event {
public:
	explicit PipelineResultReadyEvent(Executor &executor);

public:
	void Schedule() override;
	bool AutoFinishWithoutTasks() const override;
	void Open();

private:
	atomic<PipelineResultReadyEventState> state;
};

} // namespace duckdb
