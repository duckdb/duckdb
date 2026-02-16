#include "core_functions/aggregate/algebraic_functions.hpp"
#include "core_functions/aggregate/algebraic/covar.hpp"
#include "core_functions/aggregate/algebraic/stddev.hpp"
#include "core_functions/aggregate/algebraic/corr.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

LogicalType GetExportStateType(const AggregateFunction &function) {
	auto struct_children_types = child_list_t<LogicalType> {};
	struct_children_types.emplace_back("cov_pop_count", LogicalType::UBIGINT);
	struct_children_types.emplace_back("cov_pop_meanx", LogicalType::DOUBLE);
	struct_children_types.emplace_back("cov_pop_meany", LogicalType::DOUBLE);
	struct_children_types.emplace_back("cov_pop_co_moment", LogicalType::DOUBLE);

	struct_children_types.emplace_back("dev_pop_x_count", LogicalType::UBIGINT);
	struct_children_types.emplace_back("dev_pop_x_mean", LogicalType::DOUBLE);
	struct_children_types.emplace_back("dev_pop_x_dsquared", LogicalType::DOUBLE);

	struct_children_types.emplace_back("dev_pop_y_count", LogicalType::UBIGINT);
	struct_children_types.emplace_back("dev_pop_y_mean", LogicalType::DOUBLE);
	struct_children_types.emplace_back("dev_pop_y_dsquared", LogicalType::DOUBLE);
	return LogicalType::STRUCT(std::move(struct_children_types));
}

static unique_ptr<FunctionData> BindCorr(ClientContext &context, AggregateFunction &function,
                                         vector<unique_ptr<Expression>> &arguments) {
	function.SetStructStateExport(GetExportStateType);
	return nullptr;
}

AggregateFunction CorrFun::GetFunction() {
	auto func = AggregateFunction::BinaryAggregate<CorrState, double, double, double, CorrOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);

	// When 'corr' is called with non DOUBLE arguments the binder copies the AggregateFunction and applies
	// implicit casts to match the DOUBLE signature. During this copy, the get_state_type callback is lost
	// By setting this in the custom bind we can make sure it is preserved
	func.bind = BindCorr;
	func.SetStructStateExport(GetExportStateType);
	func.AllowExportStateWithCustomBinding();
	return func;
}
} // namespace duckdb
