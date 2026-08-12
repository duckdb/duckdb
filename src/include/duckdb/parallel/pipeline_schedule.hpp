//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline_schedule.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {

class MetaPipeline;
class Pipeline;

enum class PipelineScheduleStageType : uint8_t { INITIALIZE, EXECUTE, PREPARE_FINISH, FINISH, COMPLETE };
enum class PipelineScheduleMode : uint8_t { COMPLETE, ACTIVE_SUBSET };

struct PipelineScheduleDependency {
	PipelineScheduleDependency(idx_t stage_p, optional_ptr<Pipeline> external_input_consumer_p)
	    : stage(stage_p), external_input_consumer(external_input_consumer_p) {
	}

	idx_t stage;
	optional_ptr<Pipeline> external_input_consumer;
};

struct PipelineScheduleStage {
	PipelineScheduleStage(PipelineScheduleStageType type, shared_ptr<Pipeline> pipeline);

	PipelineScheduleStageType type;
	shared_ptr<Pipeline> pipeline;
	vector<PipelineScheduleDependency> dependencies;
};

struct PipelineScheduleEdge {
	PipelineScheduleEdge(idx_t dependent_p, idx_t dependency_p, optional_ptr<Pipeline> external_input_consumer_p)
	    : dependent(dependent_p), dependency(dependency_p), external_input_consumer(external_input_consumer_p) {
	}

	idx_t dependent;
	idx_t dependency;
	optional_ptr<Pipeline> external_input_consumer;
};

struct PipelineSchedule {
	vector<PipelineScheduleStage> stages;
	vector<reference<Pipeline>> initialize_on_schedule_pipelines;

	bool HasCycle() const;
	vector<PipelineScheduleEdge> GetCycle() const;
};

unique_ptr<PipelineSchedule> BuildPipelineSchedule(const vector<shared_ptr<MetaPipeline>> &meta_pipelines,
                                                   PipelineScheduleMode mode = PipelineScheduleMode::COMPLETE);

} // namespace duckdb
