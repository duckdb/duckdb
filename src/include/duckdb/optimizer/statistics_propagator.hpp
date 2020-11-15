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
	FILTER_ALWAYS_FALSE = 2,
	FILTER_TRUE_OR_NULL = 3,
	FILTER_FALSE_OR_NULL = 4
};

class StatisticsPropagator {
public:
	StatisticsPropagator(ClientContext &context);

	void PropagateStatistics(unique_ptr<LogicalOperator> &node_ptr);
private:
	//! Propagate statistics through an operator
	void PropagateStatistics(LogicalOperator &root, unique_ptr<LogicalOperator> *node_ptr);

	void PropagateStatistics(LogicalFilter &op, unique_ptr<LogicalOperator> *node_ptr);
	void PropagateStatistics(LogicalGet &op, unique_ptr<LogicalOperator> *node_ptr);
	void PropagateStatistics(LogicalJoin &op, unique_ptr<LogicalOperator> *node_ptr);
	void PropagateStatistics(LogicalProjection &op, unique_ptr<LogicalOperator> *node_ptr);
	void PropagateStatistics(LogicalComparisonJoin &op, unique_ptr<LogicalOperator> *node_ptr);
	void PropagateStatistics(LogicalAnyJoin &op, unique_ptr<LogicalOperator> *node_ptr);
	void PropagateStatistics(LogicalSetOperation &op, unique_ptr<LogicalOperator> *node_ptr);
	void PropagateStatistics(LogicalAggregate &op, unique_ptr<LogicalOperator> *node_ptr);

	void PropagateChildren(LogicalOperator &node, unique_ptr<LogicalOperator> *node_ptr);

	//! Return statistics from a constant value
	unique_ptr<BaseStatistics> StatisticsFromValue(const Value &input);
	//! Run a comparison with two sets of statistics, returns if the comparison will always returns true/false or not
	FilterPropagateResult PropagateComparison(BaseStatistics &left, BaseStatistics &right, ExpressionType comparison);

	//! Update filter statistics from a filter with a constant
	void UpdateFilterStatistics(BaseStatistics &input, ExpressionType comparison_type, Value constant);
	//! Update statistics from a filter between two stats
	void UpdateFilterStatistics(BaseStatistics &lstats, BaseStatistics &rstats, ExpressionType comparison_type);
	//! Update filter statistics from a generic comparison
	void UpdateFilterStatistics(Expression &left, Expression &right, ExpressionType comparison_type);
	//! Update filter statistics from an expression
	void UpdateFilterStatistics(Expression &condition);
	//! Set the statistics of a specific column binding to not contain null values
	void SetStatisticsNotNull(ColumnBinding binding);

	unique_ptr<BaseStatistics> PropagateExpression(unique_ptr<Expression> &expr);
	unique_ptr<BaseStatistics> PropagateExpression(Expression &expr, unique_ptr<Expression> *expr_ptr);

	unique_ptr<BaseStatistics> PropagateExpression(BoundAggregateExpression &expr, unique_ptr<Expression> *expr_ptr);
	unique_ptr<BaseStatistics> PropagateExpression(BoundBetweenExpression &expr, unique_ptr<Expression> *expr_ptr);
	unique_ptr<BaseStatistics> PropagateExpression(BoundCaseExpression &expr, unique_ptr<Expression> *expr_ptr);
	unique_ptr<BaseStatistics> PropagateExpression(BoundCastExpression &expr, unique_ptr<Expression> *expr_ptr);
	unique_ptr<BaseStatistics> PropagateExpression(BoundFunctionExpression &expr, unique_ptr<Expression> *expr_ptr);
	unique_ptr<BaseStatistics> PropagateExpression(BoundComparisonExpression &expr, unique_ptr<Expression> *expr_ptr);
	unique_ptr<BaseStatistics> PropagateExpression(BoundConstantExpression &expr, unique_ptr<Expression> *expr_ptr);
	unique_ptr<BaseStatistics> PropagateExpression(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr);
	unique_ptr<BaseStatistics> PropagateExpression(BoundOperatorExpression &expr, unique_ptr<Expression> *expr_ptr);

	void ReplaceWithEmptyResult(unique_ptr<LogicalOperator> &node);

	bool ExpressionIsConstant(Expression &expr, Value val);
	bool ExpressionIsConstantOrNull(Expression &expr, Value val);
private:
	ClientContext &context;
	//! The map of ColumnBinding -> statistics for the various nodes
	column_binding_map_t<unique_ptr<BaseStatistics>> statistics_map;
};

} // namespace duckdb
