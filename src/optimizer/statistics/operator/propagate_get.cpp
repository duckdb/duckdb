#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

void StatisticsPropagator::PropagateStatistics(LogicalGet &get, unique_ptr<LogicalOperator> *node_ptr) {
	if (!get.function.statistics) {
		// no statistics to get
		return;
	}
	for(idx_t i = 0; i < get.column_ids.size(); i++) {
		auto stats = get.function.statistics(context, get.bind_data.get(), get.column_ids[i]);
		if (stats) {
			ColumnBinding binding(get.table_index, i);
			statistics_map.insert(make_pair(binding, move(stats)));
		}
	}
	// push table filters into the statistics
	for(idx_t i = 0; i < get.tableFilters.size(); i++) {
		auto &tableFilter = get.tableFilters[i];
		// find the stats
		ColumnBinding stats_binding(get.table_index, tableFilter.column_index);
		auto entry = statistics_map.find(stats_binding);
		if (entry == statistics_map.end()) {
			// no stats for this entry
			continue;
		}
		auto constant_stats = StatisticsFromValue(tableFilter.constant);
		if (!constant_stats) {
			continue;
		}
		auto propagate_result = PropagateComparison(*entry->second, *constant_stats, tableFilter.comparison_type);
		switch(propagate_result) {
		case FilterPropagateResult::FILTER_ALWAYS_TRUE:
			// filter is always true; it is useless to execute it
			// erase this condition
			get.tableFilters.erase(get.tableFilters.begin() + i);
			i--;
			break;
		case FilterPropagateResult::FILTER_FALSE_OR_NULL:
		case FilterPropagateResult::FILTER_ALWAYS_FALSE:
			// filter is always false; this entire filter should be replaced by an empty result block
			ReplaceWithEmptyResult(*node_ptr);
			return;
		default:
			// general case: filter can be true or false, update this columns' statistics
			UpdateFilterStatistics(*entry->second, tableFilter.comparison_type, tableFilter.constant);
			break;
		}
	}
}

}
