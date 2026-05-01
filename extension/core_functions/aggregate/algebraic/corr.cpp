#include "core_functions/aggregate/algebraic_functions.hpp"
#include "core_functions/aggregate/algebraic/corr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {

LogicalType GetCorrStateType() {
	child_list_t<LogicalType> state_children;
	state_children.emplace_back("cov_pop", CovarPopFun::GetFunction().GetStateType());
	state_children.emplace_back("dev_pop_x", VarPopFun::GetFunction().GetStateType());
	state_children.emplace_back("dev_pop_y", VarPopFun::GetFunction().GetStateType());
	return LogicalType::STRUCT(std::move(state_children));
}

LogicalType GetCorrExportStateType(const AggregateFunction &) {
	return GetCorrStateType();
}

AggregateFunction CorrFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<CorrState, double, double, double, CorrOperation>(
	           LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE)
	    .SetStructStateExport(GetCorrExportStateType);
}
} // namespace duckdb
