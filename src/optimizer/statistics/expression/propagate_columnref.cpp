#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundColumnRefExpression &colref) {
	auto stats = statistics_map.find(colref.binding);
	if (stats == statistics_map.end()) {
		return nullptr;
	}
	return move(stats->second);
}

}
