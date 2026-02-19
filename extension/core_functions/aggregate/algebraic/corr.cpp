#include "core_functions/aggregate/algebraic_functions.hpp"
#include "core_functions/aggregate/algebraic/covar.hpp"
#include "core_functions/aggregate/algebraic/stddev.hpp"
#include "core_functions/aggregate/algebraic/corr.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

LogicalType GetCorrExportStateType(const AggregateFunction &function) {
	auto state_children = child_list_t<LogicalType> {};

	auto covar_pop_children = child_list_t<LogicalType> {};
	covar_pop_children.emplace_back("count", LogicalType::UBIGINT);
	covar_pop_children.emplace_back("mean_x", LogicalType::DOUBLE);
	covar_pop_children.emplace_back("mean_y", LogicalType::DOUBLE);
	covar_pop_children.emplace_back("co_moment", LogicalType::DOUBLE);
	state_children.emplace_back("cov_pop", LogicalType::STRUCT(std::move(covar_pop_children)));

	auto dev_pop_x_children = child_list_t<LogicalType> {};
	dev_pop_x_children.emplace_back("count", LogicalType::UBIGINT);
	dev_pop_x_children.emplace_back("mean", LogicalType::DOUBLE);
	dev_pop_x_children.emplace_back("dsquared", LogicalType::DOUBLE);
	state_children.emplace_back("dev_pop_x", LogicalType::STRUCT(std::move(dev_pop_x_children)));

	auto dev_pop_y_children = child_list_t<LogicalType> {};
	dev_pop_y_children.emplace_back("count", LogicalType::UBIGINT);
	dev_pop_y_children.emplace_back("mean", LogicalType::DOUBLE);
	dev_pop_y_children.emplace_back("dsquared", LogicalType::DOUBLE);
	state_children.emplace_back("dev_pop_y", LogicalType::STRUCT(std::move(dev_pop_y_children)));

	return LogicalType::STRUCT(std::move(state_children));
}

AggregateFunction CorrFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<CorrState, double, double, double, CorrOperation>(
	           LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE)
	    .SetStructStateExport(GetCorrExportStateType);
}
} // namespace duckdb
