//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline_event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

class PipelineEvent : public Event {
public:
	PipelineEvent(shared_ptr<Pipeline> pipeline);

	//! The pipeline that this event belongs to
	shared_ptr<Pipeline> pipeline;

public:
	void Schedule() override;
	void FinishEvent() override;
};

} // namespace duckdb
