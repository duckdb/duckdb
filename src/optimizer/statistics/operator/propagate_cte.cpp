#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"

namespace duckdb {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalMaterializedCTE &op,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	// the CTE is evaluated once, so the statistics of its definition hold for every reference to it
	auto definition_node_stats = PropagateStatistics(op.children[0]);

	auto definition_bindings = op.children[0]->GetColumnBindings();
	auto column_count = MinValue<idx_t>(op.column_count, definition_bindings.size());

	CTEStatistics cte_stats;
	cte_stats.column_stats.reserve(column_count);
	for (idx_t i = 0; i < column_count; i++) {
		auto entry = statistics_map.find(definition_bindings[i]);
		if (entry == statistics_map.end() || !entry->second) {
			cte_stats.column_stats.push_back(nullptr);
			continue;
		}
		cte_stats.column_stats.push_back(entry->second->ToUnique());
	}
	if (definition_node_stats) {
		cte_stats.node_stats = make_uniq<NodeStatistics>(*definition_node_stats);
	}
	cte_stats_map[op.table_index] = std::move(cte_stats);

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
	auto column_count = MinValue<idx_t>(bindings.size(), cte_stats.column_stats.size());
	for (idx_t i = 0; i < column_count; i++) {
		auto &column_stats = cte_stats.column_stats[i];
		if (!column_stats || column_stats->GetType() != op.chunk_types[i]) {
			continue;
		}
		statistics_map[bindings[i]] = column_stats->ToUnique();
	}

	// a reference scans the whole CTE, so it emits exactly as many rows as the definition
	if (!cte_stats.node_stats) {
		return nullptr;
	}
	return make_uniq<NodeStatistics>(*cte_stats.node_stats);
}

} // namespace duckdb
