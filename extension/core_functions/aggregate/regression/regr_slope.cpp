// REGR_SLOPE(y, x)
// Returns the slope of the linear regression line for non-null pairs in a group.
// It is computed for non-null pairs using the following formula:
// COVAR_POP(x,y) / VAR_POP(x)

//! Input : Any numeric type
//! Output : Double

#include "core_functions/aggregate/regression/regr_slope.hpp"
#include "duckdb/function/function_set.hpp"
#include "core_functions/aggregate/regression_functions.hpp"

namespace duckdb {

namespace {

LogicalType GetRegrSlopeStateType(const AggregateFunction &) {
	child_list_t<LogicalType> state_children;

	child_list_t<LogicalType> cov_pop_children;
	cov_pop_children.emplace_back("count", LogicalType::UBIGINT);
	cov_pop_children.emplace_back("meanx", LogicalType::DOUBLE);
	cov_pop_children.emplace_back("meany", LogicalType::DOUBLE);
	cov_pop_children.emplace_back("co_moment", LogicalType::DOUBLE);
	state_children.emplace_back("cov_pop", LogicalType::STRUCT(std::move(cov_pop_children)));

	child_list_t<LogicalType> var_pop_children;
	var_pop_children.emplace_back("count", LogicalType::UBIGINT);
	var_pop_children.emplace_back("mean", LogicalType::DOUBLE);
	var_pop_children.emplace_back("dsquared", LogicalType::DOUBLE);
	state_children.emplace_back("var_pop", LogicalType::STRUCT(std::move(var_pop_children)));

	return LogicalType::STRUCT(std::move(state_children));
}

} // namespace

AggregateFunction RegrSlopeFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrSlopeState, double, double, double, RegrSlopeOperation>(
	           LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE)
	    .SetStructStateExport(GetRegrSlopeStateType);
}

} // namespace duckdb
