#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"

namespace duckdb {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalMaterializedCTE &op,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	// the CTE is evaluated once, so the statistics of its definition hold for every reference to it
	PropagateStatistics(op.children[0]);

	auto definition_bindings = op.children[0]->GetColumnBindings();
	vector<unique_ptr<BaseStatistics>> definition_stats;
	definition_stats.reserve(definition_bindings.size());
	for (auto &binding : definition_bindings) {
		auto entry = statistics_map.find(binding);
		if (entry == statistics_map.end() || !entry->second) {
			definition_stats.push_back(nullptr);
			continue;
		}
		definition_stats.push_back(entry->second->ToUnique());
	}
	cte_stats_map[op.table_index] = std::move(definition_stats);

	// the operator emits whatever the query referencing the CTE emits
	return PropagateStatistics(op.children[1]);
}

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalCTERef &op,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	if (op.is_recurring) {
		// the recurring table is filled during execution - the definition does not describe it
		return nullptr;
	}
	auto entry = cte_stats_map.find(op.cte_index);
	if (entry == cte_stats_map.end()) {
		return nullptr;
	}
	auto &cte_stats = entry->second;

	auto bindings = op.GetColumnBindings();
	auto column_count = MinValue<idx_t>(bindings.size(), cte_stats.size());
	for (idx_t i = 0; i < column_count; i++) {
		if (!cte_stats[i] || cte_stats[i]->GetType() != op.chunk_types[i]) {
			continue;
		}
		statistics_map[bindings[i]] = cte_stats[i]->ToUnique();
	}
	return nullptr;
}

} // namespace duckdb
