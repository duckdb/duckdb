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

struct PipelineScheduleStage {
	PipelineScheduleStage(PipelineScheduleStageType type, shared_ptr<Pipeline> pipeline);

	PipelineScheduleStageType type;
	shared_ptr<Pipeline> pipeline;
	vector<idx_t> dependencies;
};

struct PipelineSchedule {
	vector<PipelineScheduleStage> stages;
	vector<reference<Pipeline>> initialize_on_schedule_pipelines;
};

unique_ptr<PipelineSchedule> BuildPipelineSchedule(const vector<shared_ptr<MetaPipeline>> &meta_pipelines,
                                                   PipelineScheduleMode mode = PipelineScheduleMode::COMPLETE);

} // namespace duckdb
