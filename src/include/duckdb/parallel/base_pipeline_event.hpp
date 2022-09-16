//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/base_pipeline_event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

//! A BasePipelineEvent is used as the basis of any event that belongs to a specific pipeline
class BasePipelineEvent : public Event {
public:
	BasePipelineEvent(shared_ptr<Pipeline> pipeline);
	BasePipelineEvent(Pipeline &pipeline);

	//! The pipeline that this event belongs to
	shared_ptr<Pipeline> pipeline;
};

} // namespace duckdb
