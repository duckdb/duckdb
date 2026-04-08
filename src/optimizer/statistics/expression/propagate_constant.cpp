#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {
class Expression;
class Value;

unique_ptr<BaseStatistics> StatisticsPropagator::StatisticsFromValue(const Value &input) {
	return BaseStatistics::FromConstant(input).ToUnique();
}

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundConstantExpression &constant,
                                                                     unique_ptr<Expression> &expr_ptr) {
	return StatisticsFromValue(constant.value);
}

} // namespace duckdb
