#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

bool StatisticsPropagator::PropagateStatistics(LogicalGet &get) {
	if (!get.function.statistics) {
		// no statistics to get
		return false;
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
		auto propagate_result = PropagateFilter(ColumnBinding(get.table_index, tableFilter.column_index), tableFilter.comparison_type, tableFilter.constant);
		if (propagate_result == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
			// filter is always true; it is useless to execute it
			// erase this condition
			get.tableFilters.erase(get.tableFilters.begin() + i);
			i--;
		} else if (propagate_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
			// filter is always false; this entire filter should be replaced by an empty result block
			return true;
		}
	}
	return false;
}

}
