//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline_event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/base_pipeline_event.hpp"

namespace duckdb {

//! A PipelineEvent is responsible for scheduling a pipeline
class PipelineEvent : public BasePipelineEvent {
public:
	PipelineEvent(shared_ptr<Pipeline> pipeline);

public:
	void Schedule() override;
	void FinishEvent() override;
};

} // namespace duckdb
