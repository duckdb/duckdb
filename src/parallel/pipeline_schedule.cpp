#include "duckdb/parallel/pipeline_schedule.hpp"

#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"

namespace duckdb {

struct PipelineScheduleStageStack {
	PipelineScheduleStageStack(idx_t initialize_p, idx_t execute_p, idx_t prepare_finish_p, idx_t finish_p,
	                           idx_t complete_p)
	    : initialize(initialize_p), execute(execute_p), prepare_finish(prepare_finish_p), finish(finish_p),
	      complete(complete_p) {
	}

	idx_t initialize;
	idx_t execute;
	idx_t prepare_finish;
	idx_t finish;
	idx_t complete;
};

using pipeline_schedule_stage_map_t = reference_map_t<Pipeline, PipelineScheduleStageStack>;

PipelineScheduleStage::PipelineScheduleStage(PipelineScheduleStageType type_p, shared_ptr<Pipeline> pipeline_p)
    : type(type_p), pipeline(std::move(pipeline_p)) {
}

static idx_t AddStage(PipelineSchedule &result, PipelineScheduleStageType type, const shared_ptr<Pipeline> &pipeline) {
	result.stages.emplace_back(type, pipeline);
	return result.stages.size() - 1;
}

static void AddDependency(PipelineSchedule &result, idx_t dependent, idx_t dependency) {
	result.stages[dependent].dependencies.push_back(dependency);
}

static PipelineScheduleStageStack AddBasePipeline(PipelineSchedule &result, const shared_ptr<Pipeline> &pipeline) {
	auto initialize = AddStage(result, PipelineScheduleStageType::INITIALIZE, pipeline);
	auto execute = AddStage(result, PipelineScheduleStageType::EXECUTE, pipeline);
	auto prepare_finish = AddStage(result, PipelineScheduleStageType::PREPARE_FINISH, pipeline);
	auto finish = AddStage(result, PipelineScheduleStageType::FINISH, pipeline);
	auto complete = AddStage(result, PipelineScheduleStageType::COMPLETE, pipeline);
	// Dependencies: initialize -> execute -> prepare finish -> finish -> complete.
	AddDependency(result, execute, initialize);
	AddDependency(result, prepare_finish, execute);
	AddDependency(result, finish, prepare_finish);
	AddDependency(result, complete, finish);
	return PipelineScheduleStageStack(initialize, execute, prepare_finish, finish, complete);
}

static bool RequiresInitializeOnSchedule(Pipeline &pipeline) {
	auto source = pipeline.GetSource();
	if (!source || source->type != PhysicalOperatorType::TABLE_SCAN) {
		return false;
	}
	auto &table_function = source->Cast<PhysicalTableScan>();
	return table_function.function.global_initialization == TableFunctionInitialization::INITIALIZE_ON_SCHEDULE;
}

static void AddMetaPipeline(PipelineSchedule &result, MetaPipeline &meta_pipeline,
                            pipeline_schedule_stage_map_t &stage_map) {
	vector<shared_ptr<Pipeline>> pipelines;
	meta_pipeline.GetPipelines(pipelines, false);
	D_ASSERT(!pipelines.empty());

	auto base_stack = AddBasePipeline(result, pipelines[0]);
	stage_map.emplace(*pipelines[0], base_stack);

	for (idx_t pipeline_idx = 1; pipeline_idx < pipelines.size(); pipeline_idx++) {
		auto &pipeline = pipelines[pipeline_idx];
		auto execute = AddStage(result, PipelineScheduleStageType::EXECUTE, pipeline);
		auto finish_group = meta_pipeline.GetFinishGroup(*pipeline);
		if (finish_group) {
			// This pipeline is part of a finish group.
			auto group_entry = stage_map.find(*finish_group);
			D_ASSERT(group_entry != stage_map.end());
			auto &group_stack = group_entry->second;
			// Dependencies: base finish -> pipeline execute -> group prepare finish.
			AddDependency(result, execute, base_stack.finish);
			AddDependency(result, group_stack.prepare_finish, execute);
			stage_map.emplace(*pipeline,
			                  PipelineScheduleStageStack(base_stack.initialize, execute, group_stack.prepare_finish,
			                                             group_stack.finish, base_stack.complete));
		} else if (meta_pipeline.HasFinishEvent(*pipeline)) {
			// This pipeline has its own finish event despite writing to the shared sink, so Finalize runs twice.
			auto prepare_finish = AddStage(result, PipelineScheduleStageType::PREPARE_FINISH, pipeline);
			auto finish = AddStage(result, PipelineScheduleStageType::FINISH, pipeline);
			// Dependencies:
			// base finish -> pipeline execute -> pipeline prepare finish -> pipeline finish -> base complete.
			AddDependency(result, execute, base_stack.finish);
			AddDependency(result, prepare_finish, execute);
			AddDependency(result, finish, prepare_finish);
			AddDependency(result, base_stack.complete, finish);
			stage_map.emplace(*pipeline, PipelineScheduleStageStack(base_stack.initialize, execute, prepare_finish,
			                                                        finish, base_stack.complete));
		} else {
			// No additional finish event: execute after base initialization and join the base finish chain.
			AddDependency(result, execute, base_stack.initialize);
			AddDependency(result, base_stack.prepare_finish, execute);
			stage_map.emplace(*pipeline,
			                  PipelineScheduleStageStack(base_stack.initialize, execute, base_stack.prepare_finish,
			                                             base_stack.finish, base_stack.complete));
		}
	}

	for (auto &pipeline : pipelines) {
		if (RequiresInitializeOnSchedule(*pipeline)) {
			// Certain functions must be eagerly initialized during scheduling. Record them here so the schedule
			// consumer initializes the function before it starts scheduling events.
			result.initialize_on_schedule_pipelines.push_back(*pipeline);
		}
	}
}

unique_ptr<PipelineSchedule> BuildPipelineSchedule(const vector<shared_ptr<MetaPipeline>> &meta_pipelines,
                                                   PipelineScheduleMode mode) {
	auto result = make_uniq<PipelineSchedule>();
	pipeline_schedule_stage_map_t stage_map;
	const bool allow_missing_meta_pipelines = mode == PipelineScheduleMode::ACTIVE_SUBSET;

	for (auto &meta_pipeline : meta_pipelines) {
		AddMetaPipeline(*result, *meta_pipeline, stage_map);
	}

	for (auto &entry : stage_map) {
		auto &pipeline = entry.first.get();
		// A pipeline dependency is complete only after its sink has finalized.
		for (auto &dependency : pipeline.GetDependencies()) {
			auto dep = dependency.lock();
			D_ASSERT(dep);
			auto dep_entry = stage_map.find(*dep);
			if (dep_entry != stage_map.end()) {
				AddDependency(*result, entry.second.execute, dep_entry->second.complete);
			}
		}
		// A dataflow dependency only needs the producer's source and sink state to be initialized.
		for (auto &dependency : pipeline.GetDataflowDependencies()) {
			auto dep = dependency.lock();
			D_ASSERT(dep);
			auto dep_entry = stage_map.find(*dep);
			if (dep_entry != stage_map.end()) {
				AddDependency(*result, entry.second.execute, dep_entry->second.initialize);
			}
		}
		// External finish dependencies block both execution and PrepareFinalize on the producer's execution.
		for (auto &dependency : pipeline.GetExternalFinishDependencies()) {
			auto dep = dependency.lock();
			D_ASSERT(dep);
			auto dep_entry = stage_map.find(*dep);
			if (dep_entry != stage_map.end()) {
				AddDependency(*result, entry.second.execute, dep_entry->second.execute);
				AddDependency(*result, entry.second.prepare_finish, dep_entry->second.execute);
			}
		}
	}

	// Meta-pipeline dependencies order their execute stages directly.
	for (auto &meta_pipeline : meta_pipelines) {
		for (auto &entry : meta_pipeline->GetDependencies()) {
			auto pipeline_entry = stage_map.find(entry.first.get());
			if (pipeline_entry == stage_map.end()) {
				if (!allow_missing_meta_pipelines) {
					throw InternalException("Missing pipeline in meta-pipeline dependency");
				}
				continue;
			}
			for (auto &dependency : entry.second) {
				auto dependency_entry = stage_map.find(dependency.get());
				if (dependency_entry == stage_map.end()) {
					if (!allow_missing_meta_pipelines) {
						throw InternalException("Missing dependency pipeline in meta-pipeline dependency");
					}
					continue;
				}
				AddDependency(*result, pipeline_entry->second.execute, dependency_entry->second.execute);
			}
		}
	}

	// These dependencies make sibling join builds run in this order:
	// 1. all child pipelines execute through Combine
	// 2. all child pipelines run PrepareFinalize
	// 3. all child pipelines run Finalize
	// Operators communicate their memory usage through the TemporaryMemoryManager in PrepareFinalize. When the
	// child pipelines Finalize, all required memory is known and the manager can make an informed decision.
	for (auto &meta_pipeline : meta_pipelines) {
		vector<shared_ptr<MetaPipeline>> children;
		meta_pipeline->GetMetaPipelines(children, false, true);
		for (auto &child1 : children) {
			if (child1->Type() != MetaPipelineType::JOIN_BUILD) {
				continue;
			}
			auto child1_entry = stage_map.find(*child1->GetBasePipeline());
			if (child1_entry == stage_map.end()) {
				if (!allow_missing_meta_pipelines) {
					throw InternalException("Missing sibling join-build pipeline");
				}
				continue;
			}

			for (auto &child2 : children) {
				if (child2->Type() != MetaPipelineType::JOIN_BUILD || RefersToSameObject(*child1, *child2)) {
					continue;
				}
				if (!RefersToSameObject(*child1->GetParent(), *child2->GetParent())) {
					continue;
				}
				auto child2_entry = stage_map.find(*child2->GetBasePipeline());
				if (child2_entry == stage_map.end()) {
					if (!allow_missing_meta_pipelines) {
						throw InternalException("Missing sibling join-build dependency pipeline");
					}
					continue;
				}
				// All children must finish Combine before any child enters PrepareFinalize.
				AddDependency(*result, child1_entry->second.prepare_finish, child2_entry->second.execute);
				// All children must finish PrepareFinalize before any child enters Finalize.
				AddDependency(*result, child1_entry->second.finish, child2_entry->second.prepare_finish);
			}
		}
	}
	return result;
}

} // namespace duckdb
