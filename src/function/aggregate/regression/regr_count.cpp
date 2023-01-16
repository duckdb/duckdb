#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/regression_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/function/aggregate/regression/regr_count.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

void RegrCountFun::RegisterFunction(BuiltinFunctions &set) {
	auto regr_count = AggregateFunction::BinaryAggregate<size_t, double, double, uint32_t, RegrCountFunction>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::UINTEGER);
	regr_count.name = "regr_count";
	regr_count.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction(regr_count);
}

} // namespace duckdb
