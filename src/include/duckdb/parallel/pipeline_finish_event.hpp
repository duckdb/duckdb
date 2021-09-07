//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline_finish_event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/pipeline.hpp"


namespace duckdb {
class Executor;

class PipelineFinishEvent : public Event {
public:
	PipelineFinishEvent(const shared_ptr<Pipeline> &pipeline);

	//! The pipeline that this event belongs to
	weak_ptr<Pipeline> pipeline_w;
public:
	void Schedule() override;
	void FinishEvent() override;
};

}
