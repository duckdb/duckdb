// REGR_SLOPE(y, x)
// Returns the slope of the linear regression line for non-null pairs in a group.
// It is computed for non-null pairs using the following formula:
// COVAR_POP(x,y) / VAR_POP(x)

//! Input : Any numeric type
//! Output : Double

#include "core_functions/aggregate/regression/regr_slope.hpp"
#include "core_functions/aggregate/algebraic_functions.hpp"
#include "core_functions/aggregate/regression_functions.hpp"

namespace duckdb {

namespace {

LogicalType GetRegrSlopeStateType(const BoundAggregateFunction &) {
	child_list_t<LogicalType> covar_children;
	covar_children.emplace_back("count", LogicalType::UBIGINT);
	covar_children.emplace_back("meanx", LogicalType::DOUBLE);
	covar_children.emplace_back("meany", LogicalType::DOUBLE);
	covar_children.emplace_back("co_moment", LogicalType::DOUBLE);
	auto cov_pop_type = LogicalType::STRUCT(std::move(covar_children));

	child_list_t<LogicalType> stddev_types;
	stddev_types.emplace_back("count", LogicalType::UBIGINT);
	stddev_types.emplace_back("mean", LogicalType::DOUBLE);
	stddev_types.emplace_back("dsquared", LogicalType::DOUBLE);
	auto stddev_type = LogicalType::STRUCT(std::move(stddev_types));

	child_list_t<LogicalType> state_children;
	state_children.emplace_back("cov_pop", std::move(cov_pop_type));
	state_children.emplace_back("var_pop", std::move(stddev_type));
	return LogicalType::STRUCT(std::move(state_children));
}

} // namespace

AggregateFunction RegrSlopeFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrSlopeState, double, double, double, RegrSlopeOperation>(
	           LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE)
	    .SetStructStateExport(GetRegrSlopeStateType);
}

} // namespace duckdb
