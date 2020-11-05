//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/statistics_propagator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/logical_tokens.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/bound_tokens.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
class ClientContext;
class LogicalOperator;

class StatisticsPropagator {
public:
	StatisticsPropagator(ClientContext &context);

	void PropagateStatistics(LogicalOperator &root);
private:
	void PropagateStatistics(LogicalFilter &op);
	void PropagateStatistics(LogicalGet &op);
	void PropagateStatistics(LogicalProjection &op);

	unique_ptr<BaseStatistics> PropagateExpression(Expression &expr);

	unique_ptr<BaseStatistics> PropagateExpression(BoundCastExpression &expr);
	unique_ptr<BaseStatistics> PropagateExpression(BoundFunctionExpression &expr);
	unique_ptr<BaseStatistics> PropagateExpression(BoundConstantExpression &expr);
	unique_ptr<BaseStatistics> PropagateExpression(BoundColumnRefExpression &expr);
private:
	ClientContext &context;
	//! The map of ColumnBinding -> statistics for the various nodes
	column_binding_map_t<unique_ptr<BaseStatistics>> statistics_map;
};

} // namespace duckdb
