//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline_complete_event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/event.hpp"

namespace duckdb {
class Executor;

class PipelineCompleteEvent : public Event {
public:
	PipelineCompleteEvent(Executor &executor);

public:
	void Schedule() override;
	void FinalizeFinish() override;
};

}
