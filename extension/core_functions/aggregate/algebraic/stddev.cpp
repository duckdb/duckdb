#include "core_functions/aggregate/algebraic_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/function_set.hpp"
#include "core_functions/aggregate/algebraic/stddev.hpp"
#include <cmath>

namespace duckdb {

static LogicalType GetStddevExportStateType(const AggregateFunction &function) {
	child_list_t<LogicalType> struct_children;
	struct_children.emplace_back("count", LogicalType::UBIGINT);
	struct_children.emplace_back("mean", LogicalType::DOUBLE);
	struct_children.emplace_back("dsquared", LogicalType::DOUBLE);

	return LogicalType::STRUCT(std::move(struct_children));
}

AggregateFunction StdDevSampFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<StddevState, double, double, STDDevSampOperation>(LogicalType::DOUBLE,
	                                                                                           LogicalType::DOUBLE)
	    .SetStructStateExport(GetStddevExportStateType);
}

AggregateFunction StdDevPopFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<StddevState, double, double, STDDevPopOperation>(LogicalType::DOUBLE,
	                                                                                          LogicalType::DOUBLE)
	    .SetStructStateExport(GetStddevExportStateType);
}

AggregateFunction VarPopFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<StddevState, double, double, VarPopOperation>(LogicalType::DOUBLE,
	                                                                                       LogicalType::DOUBLE)
	    .SetStructStateExport(GetStddevExportStateType);
}

AggregateFunction VarSampFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<StddevState, double, double, VarSampOperation>(LogicalType::DOUBLE,
	                                                                                        LogicalType::DOUBLE)
	    .SetStructStateExport(GetStddevExportStateType);
}

AggregateFunction StandardErrorOfTheMeanFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<StddevState, double, double, StandardErrorOfTheMeanOperation>(
	           LogicalType::DOUBLE, LogicalType::DOUBLE)
	    .SetStructStateExport(GetStddevExportStateType);
}

} // namespace duckdb
