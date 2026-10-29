#include "duckdb/parallel/pipeline_schedule.hpp"

#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

static optional_ptr<PhysicalComparisonJoin> RuntimeFilterProducer(Pipeline &pipeline) {
	auto sink = pipeline.GetSink();
	if (!sink) {
		return nullptr;
	}
	switch (sink->type) {
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		auto &join = sink->Cast<PhysicalComparisonJoin>();
		return join.filter_pushdown && !join.filter_pushdown->probe_info.empty() ? &join : nullptr;
	}
	default:
		return nullptr;
	}
}

void ScheduleRuntimeFilters(PipelineSchedule &schedule) {
	reference_map_t<PhysicalComparisonJoin, vector<idx_t>> publications;
	vector<reference<PhysicalComparisonJoin>> producers;
	for (idx_t stage_idx = 0; stage_idx < schedule.stages.size(); stage_idx++) {
		auto &stage = schedule.stages[stage_idx];
		if (stage.type != PipelineScheduleStageType::COMPLETE) {
			continue;
		}
		auto producer = RuntimeFilterProducer(*stage.pipeline);
		if (producer) {
			auto &stages = publications[*producer];
			if (stages.empty()) {
				producers.push_back(*producer);
			}
			stages.push_back(stage_idx);
		}
	}
	if (publications.empty()) {
		return;
	}

	reference_map_t<DynamicTableFilterSet, vector<idx_t>> sources;
	for (idx_t stage_idx = 0; stage_idx < schedule.stages.size(); stage_idx++) {
		auto &stage = schedule.stages[stage_idx];
		auto &pipeline = *stage.pipeline;
		auto source = pipeline.GetSource();
		if (stage.type != PipelineScheduleStageType::EXECUTE || !source ||
		    source->type != PhysicalOperatorType::TABLE_SCAN || pipeline.HasSourceState() ||
		    pipeline.IsExternalInput()) {
			continue;
		}
		auto &scan = source->Cast<PhysicalTableScan>();
		if (!scan.dynamic_filters ||
		    scan.function.global_initialization == TableFunctionInitialization::INITIALIZE_ON_SCHEDULE ||
		    (scan.table_filters && (scan.table_filters->HasFilters() || scan.table_filters->HasMultiColumnFilters()))) {
			continue;
		}
		sources[*scan.dynamic_filters].push_back(stage_idx);
	}

	for (auto &producer_ref : producers) {
		auto &producer = producer_ref.get();
		auto &publication = publications[producer];
		auto build_estimate = producer.children[1].get().estimated_cardinality;
		auto probe_estimate = producer.children[0].get().estimated_cardinality;
		for (auto &target : producer.filter_pushdown->probe_info) {
			// Clear previous executions before any scan can take its filter snapshot.
			target.dynamic_filters->ClearFilters(producer);
			auto source = sources.find(*target.dynamic_filters);
			if (publication.size() != 1 || source == sources.end() ||
			    build_estimate >= MaxValue(probe_estimate, target.estimated_scan_cardinality)) {
				continue;
			}
			for (auto execution : source->second) {
				schedule.TryAddDependency(execution, publication[0]);
			}
		}
	}
}

} // namespace duckdb
