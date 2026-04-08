#include <unordered_map>
#include <utility>

#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {
class Expression;

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundColumnRefExpression &colref,
                                                                     unique_ptr<Expression> &expr_ptr) {
	auto stats = statistics_map.find(colref.binding);
	if (stats == statistics_map.end()) {
		return nullptr;
	}
	return stats->second->ToUnique();
}

} // namespace duckdb
