#include <stdint.h>
#include <string>
#include <utility>

#include "core_functions/aggregate/regression_functions.hpp"
#include "core_functions/aggregate/regression/regr_count.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {

namespace {

LogicalType GetRegrCountStateType(const AggregateFunction &) {
	child_list_t<LogicalType> child_types;
	child_types.emplace_back("count", LogicalType::UBIGINT);
	return LogicalType::STRUCT(std::move(child_types));
}

} // namespace

AggregateFunction RegrCountFun::GetFunction() {
	auto regr_count = AggregateFunction::BinaryAggregate<uint64_t, double, double, uint32_t, RegrCountFunction>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::UINTEGER);
	regr_count.name = "regr_count";
	regr_count.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return regr_count.SetStructStateExport(GetRegrCountStateType);
}

} // namespace duckdb
