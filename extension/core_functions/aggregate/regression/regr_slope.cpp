// REGR_SLOPE(y, x)
// Returns the slope of the linear regression line for non-null pairs in a group.
// It is computed for non-null pairs using the following formula:
// COVAR_POP(x,y) / VAR_POP(x)

//! Input : Any numeric type
//! Output : Double

#include "core_functions/aggregate/regression/regr_slope.hpp"

#include "core_functions/aggregate/algebraic_functions.hpp"
#include "core_functions/aggregate/regression_functions.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {

namespace {

LogicalType GetRegrSlopeStateType(const AggregateFunction &) {
	child_list_t<LogicalType> state_children;
	state_children.emplace_back("cov_pop", CovarPopFun::GetFunction().GetStateType());
	state_children.emplace_back("var_pop", VarPopFun::GetFunction().GetStateType());
	return LogicalType::STRUCT(std::move(state_children));
}

} // namespace

AggregateFunction RegrSlopeFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrSlopeState, double, double, double, RegrSlopeOperation>(
	           LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE)
	    .SetStructStateExport(GetRegrSlopeStateType);
}

} // namespace duckdb
