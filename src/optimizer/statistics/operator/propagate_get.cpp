#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

void StatisticsPropagator::PropagateStatistics(LogicalGet &get) {
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
}

}
