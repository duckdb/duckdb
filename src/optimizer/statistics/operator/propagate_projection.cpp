#include <memory>
#include <utility>

#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/projection_index.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

namespace duckdb {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalProjection &proj,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	// first propagate to the child
	node_stats = PropagateStatistics(proj.children[0]);
	if (proj.children[0]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
		ReplaceWithEmptyResult(node_ptr);
		return std::move(node_stats);
	}
	// then propagate to each of the expressions
	for (idx_t i = 0; i < proj.expressions.size(); i++) {
		auto stats = PropagateExpression(proj.expressions[i]);
		if (stats) {
			ColumnBinding binding(proj.table_index, ProjectionIndex(i));
			statistics_map.insert(make_pair(binding, std::move(stats)));
		}
	}
	return std::move(node_stats);
}

} // namespace duckdb
