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

enum class FilterPropagateResult : uint8_t {
	NO_PRUNING_POSSIBLE = 0,
	FILTER_ALWAYS_TRUE = 1,
	FILTER_ALWAYS_FALSE = 2
};

class StatisticsPropagator {
public:
	StatisticsPropagator(ClientContext &context);

	//! Propagate statistics through an operator; returns true if the operator should be replaced with a LogicalEmptyResult
	bool PropagateStatistics(LogicalOperator &root);
private:
	bool PropagateStatistics(LogicalFilter &op);
	bool PropagateStatistics(LogicalGet &op);
	bool PropagateStatistics(LogicalProjection &op);

	//! Propagate a filter condition down to the statistics.
	FilterPropagateResult PropagateFilter(Expression &expr);
	FilterPropagateResult PropagateFilter(ColumnBinding binding, ExpressionType comparison, Value constant);

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
