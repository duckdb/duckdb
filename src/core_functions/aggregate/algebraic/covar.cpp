#include "duckdb/core_functions/aggregate/algebraic_functions.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/core_functions/aggregate/algebraic/covar.hpp"

namespace duckdb {

AggregateFunction CovarPopFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<CovarState, double, double, double, CovarPopOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}

AggregateFunction CovarSampFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<CovarState, double, double, double, CovarSampOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}

} // namespace duckdb
