#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalGet &get,
                                                                     unique_ptr<LogicalOperator> *node_ptr) {
	if (get.function.cardinality) {
		node_stats = get.function.cardinality(context, get.bind_data.get());
	}
	if (!get.function.statistics) {
		// no column statistics to get
		return move(node_stats);
	}
	for (idx_t i = 0; i < get.column_ids.size(); i++) {
		auto stats = get.function.statistics(context, get.bind_data.get(), get.column_ids[i]);
		if (stats) {
			ColumnBinding binding(get.table_index, i);
			statistics_map.insert(make_pair(binding, move(stats)));
		}
	}
	// push table filters into the statistics
	for (idx_t i = 0; i < get.table_filters.size(); i++) {
		auto &table_filter = get.table_filters[i];
		idx_t column_index;
		for (column_index = 0; column_index < get.column_ids.size(); column_index++) {
			if (get.column_ids[column_index] == table_filter.column_index) {
				break;
			}
		}
		D_ASSERT(get.column_ids[column_index] == table_filter.column_index);
		// find the stats
		ColumnBinding stats_binding(get.table_index, column_index);
		auto entry = statistics_map.find(stats_binding);
		if (entry == statistics_map.end()) {
			// no stats for this entry
			continue;
		}
		auto constant_stats = StatisticsFromValue(table_filter.constant);
		if (!constant_stats) {
			continue;
		}
		auto propagate_result = PropagateComparison(*entry->second, *constant_stats, table_filter.comparison_type);
		switch (propagate_result) {
		case FilterPropagateResult::FILTER_ALWAYS_TRUE:
			// filter is always true; it is useless to execute it
			// erase this condition
			get.table_filters.erase(get.table_filters.begin() + i);
			i--;
			break;
		case FilterPropagateResult::FILTER_FALSE_OR_NULL:
		case FilterPropagateResult::FILTER_ALWAYS_FALSE:
			// filter is always false; this entire filter should be replaced by an empty result block
			ReplaceWithEmptyResult(*node_ptr);
			return make_unique<NodeStatistics>(0, 0);
		default:
			// general case: filter can be true or false, update this columns' statistics
			UpdateFilterStatistics(*entry->second, table_filter.comparison_type, table_filter.constant);
			break;
		}
	}
	return move(node_stats);
}

} // namespace duckdb
