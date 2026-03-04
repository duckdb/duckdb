#include "core_functions/aggregate/algebraic_functions.hpp"
#include "core_functions/aggregate/algebraic/covar.hpp"

namespace duckdb {

namespace {

LogicalType GetCovarStateType(const AggregateFunction &) {
	child_list_t<LogicalType> child_types;
	child_types.emplace_back("count", LogicalType::UBIGINT);
	child_types.emplace_back("meanx", LogicalType::DOUBLE);
	child_types.emplace_back("meany", LogicalType::DOUBLE);
	child_types.emplace_back("co_moment", LogicalType::DOUBLE);
	return LogicalType::STRUCT(std::move(child_types));
}

} // namespace

AggregateFunction CovarPopFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<CovarState, double, double, double, CovarPopOperation>(
	           LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE)
	    .SetStructStateExport(GetCovarStateType);
}

AggregateFunction CovarSampFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<CovarState, double, double, double, CovarSampOperation>(
	           LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE)
	    .SetStructStateExport(GetCovarStateType);
}

} // namespace duckdb
