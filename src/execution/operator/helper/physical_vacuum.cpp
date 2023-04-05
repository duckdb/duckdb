#include "duckdb/execution/operator/helper/physical_vacuum.hpp"

#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/statistics/distinct_statistics.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

PhysicalVacuum::PhysicalVacuum(unique_ptr<VacuumInfo> info_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::VACUUM, {LogicalType::BOOLEAN}, estimated_cardinality),
      info(std::move(info_p)) {
}

class VacuumLocalSinkState : public LocalSinkState {
public:
	explicit VacuumLocalSinkState(VacuumInfo &info) {
		for (idx_t col_idx = 0; col_idx < info.columns.size(); col_idx++) {
			column_distinct_stats.push_back(make_uniq<DistinctStatistics>());
		}
	};

	vector<unique_ptr<DistinctStatistics>> column_distinct_stats;
};

unique_ptr<LocalSinkState> PhysicalVacuum::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<VacuumLocalSinkState>(*info);
}

class VacuumGlobalSinkState : public GlobalSinkState {
public:
	explicit VacuumGlobalSinkState(VacuumInfo &info) {
		for (idx_t col_idx = 0; col_idx < info.columns.size(); col_idx++) {
			column_distinct_stats.push_back(make_uniq<DistinctStatistics>());
		}
	};

	mutex stats_lock;
	vector<unique_ptr<DistinctStatistics>> column_distinct_stats;
};

unique_ptr<GlobalSinkState> PhysicalVacuum::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<VacuumGlobalSinkState>(*info);
}

SinkResultType PhysicalVacuum::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                    DataChunk &input) const {
	auto &lstate = lstate_p.Cast<VacuumLocalSinkState>();
	D_ASSERT(lstate.column_distinct_stats.size() == info->column_id_map.size());

	for (idx_t col_idx = 0; col_idx < input.data.size(); col_idx++) {
		if (!DistinctStatistics::TypeIsSupported(input.data[col_idx].GetType())) {
			continue;
		}
		lstate.column_distinct_stats[col_idx]->Update(input.data[col_idx], input.size(), false);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalVacuum::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &gstate = gstate_p.Cast<VacuumGlobalSinkState>();
	auto &lstate = lstate_p.Cast<VacuumLocalSinkState>();

	lock_guard<mutex> lock(gstate.stats_lock);
	D_ASSERT(gstate.column_distinct_stats.size() == lstate.column_distinct_stats.size());
	for (idx_t col_idx = 0; col_idx < gstate.column_distinct_stats.size(); col_idx++) {
		gstate.column_distinct_stats[col_idx]->Merge(*lstate.column_distinct_stats[col_idx]);
	}
}

SinkFinalizeType PhysicalVacuum::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          GlobalSinkState &gstate) const {
	auto &sink = gstate.Cast<VacuumGlobalSinkState>();

	auto table = info->table;
	for (idx_t col_idx = 0; col_idx < sink.column_distinct_stats.size(); col_idx++) {
		table->GetStorage().SetDistinct(info->column_id_map.at(col_idx),
		                                std::move(sink.column_distinct_stats[col_idx]));
	}

	return SinkFinalizeType::READY;
}

void PhysicalVacuum::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	// NOP
}

} // namespace duckdb
