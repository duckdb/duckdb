#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/regression_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/function/aggregate/regression/regr_count.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

void RegrCountFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet corr("regr_count");
	corr.AddFunction(AggregateFunction::BinaryAggregate<size_t, double, double, uint32_t, RegrCountFunction>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::UINTEGER));
	set.AddFunction(corr);
}

} // namespace duckdb
