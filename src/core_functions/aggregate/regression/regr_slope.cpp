// REGR_SLOPE(y, x)
// Returns the slope of the linear regression line for non-null pairs in a group.
// It is computed for non-null pairs using the following formula:
// COVAR_POP(x,y) / VAR_POP(x)

//! Input : Any numeric type
//! Output : Double

#include "duckdb/core_functions/aggregate/regression/regr_slope.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/core_functions/aggregate/regression_functions.hpp"

namespace duckdb {

AggregateFunction RegrSlopeFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrSlopeState, double, double, double, RegrSlopeOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}

} // namespace duckdb
