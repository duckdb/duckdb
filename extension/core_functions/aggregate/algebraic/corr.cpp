#include "core_functions/aggregate/algebraic_functions.hpp"
#include "core_functions/aggregate/algebraic/covar.hpp"
#include "core_functions/aggregate/algebraic/stddev.hpp"
#include "core_functions/aggregate/algebraic/corr.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

LogicalType GetCorrStateType() {
	child_list_t<LogicalType> covar_children;
	covar_children.emplace_back("count", LogicalType::UBIGINT);
	covar_children.emplace_back("meanx", LogicalType::DOUBLE);
	covar_children.emplace_back("meany", LogicalType::DOUBLE);
	covar_children.emplace_back("co_moment", LogicalType::DOUBLE);
	auto cov_pop_type = LogicalType::STRUCT(std::move(covar_children));

	child_list_t<LogicalType> stddev_types;
	stddev_types.emplace_back("count", LogicalType::UBIGINT);
	stddev_types.emplace_back("mean", LogicalType::DOUBLE);
	stddev_types.emplace_back("dsquared", LogicalType::DOUBLE);
	auto stddev_type = LogicalType::STRUCT(std::move(stddev_types));

	child_list_t<LogicalType> state_children;
	state_children.emplace_back("cov_pop", std::move(cov_pop_type));
	state_children.emplace_back("dev_pop_x", stddev_type);
	state_children.emplace_back("dev_pop_y", stddev_type);
	return LogicalType::STRUCT(std::move(state_children));
}

LogicalType GetCorrExportStateType(const BoundAggregateFunction &) {
	return GetCorrStateType();
}

AggregateFunction CorrFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<CorrState, double, double, double, CorrOperation>(
	           LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE)
	    .SetStructStateExport(GetCorrExportStateType);
}
} // namespace duckdb
