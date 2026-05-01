#include "core_functions/aggregate/algebraic_functions.hpp"
#include "core_functions/aggregate/algebraic/stddev.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {

namespace {

LogicalType GetStddevStateType(const AggregateFunction &) {
	child_list_t<LogicalType> child_types;
	child_types.emplace_back("count", LogicalType::UBIGINT);
	child_types.emplace_back("mean", LogicalType::DOUBLE);
	child_types.emplace_back("dsquared", LogicalType::DOUBLE);
	return LogicalType::STRUCT(std::move(child_types));
}

} // namespace

AggregateFunction StdDevSampFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<StddevState, double, double, STDDevSampOperation>(LogicalType::DOUBLE,
	                                                                                           LogicalType::DOUBLE)
	    .SetStructStateExport(GetStddevStateType);
}

AggregateFunction StdDevPopFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<StddevState, double, double, STDDevPopOperation>(LogicalType::DOUBLE,
	                                                                                          LogicalType::DOUBLE)
	    .SetStructStateExport(GetStddevStateType);
}

AggregateFunction VarPopFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<StddevState, double, double, VarPopOperation>(LogicalType::DOUBLE,
	                                                                                       LogicalType::DOUBLE)
	    .SetStructStateExport(GetStddevStateType);
}

AggregateFunction VarSampFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<StddevState, double, double, VarSampOperation>(LogicalType::DOUBLE,
	                                                                                        LogicalType::DOUBLE)
	    .SetStructStateExport(GetStddevStateType);
}

AggregateFunction StandardErrorOfTheMeanFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<StddevState, double, double, StandardErrorOfTheMeanOperation>(
	           LogicalType::DOUBLE, LogicalType::DOUBLE)
	    .SetStructStateExport(GetStddevStateType);
}

} // namespace duckdb
