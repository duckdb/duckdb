#include "core_functions/aggregate/algebraic_functions.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "core_functions/aggregate/algebraic/covar.hpp"

namespace duckdb {

static LogicalType GetCovarExportStateType(const AggregateFunction &function) {
	child_list_t<LogicalType> struct_children;
	struct_children.emplace_back("count", LogicalType::UINTEGER);
	struct_children.emplace_back("mean_x", LogicalType::DOUBLE);
	struct_children.emplace_back("mean_y", LogicalType::DOUBLE);
	struct_children.emplace_back("co_moment", LogicalType::DOUBLE);

	return LogicalType::STRUCT(std::move(struct_children));
}

AggregateFunction CovarPopFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<CovarState, double, double, double, CovarPopOperation>(
	           LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE)
	    .SetStructStateExport(GetCovarExportStateType);
}

AggregateFunction CovarSampFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<CovarState, double, double, double, CovarSampOperation>(
	           LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE)
	    .SetStructStateExport(GetCovarExportStateType);
}

} // namespace duckdb
