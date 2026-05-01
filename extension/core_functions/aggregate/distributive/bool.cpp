#include "core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/aggregate_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

namespace {

struct BoolState {
	bool empty;
	bool val;
};

using BoolAndFunFunction = EmptyValAggregate<LogicalAnd, ConstantInit<true>>;
using BoolOrFunFunction = EmptyValAggregate<LogicalOr, ConstantInit<false>>;

LogicalType GetBoolAndStateType(const AggregateFunction &function) {
	child_list_t<LogicalType> child_types;
	child_types.emplace_back("empty", LogicalType::BOOLEAN);
	child_types.emplace_back("val", LogicalType::BOOLEAN);
	return LogicalType::STRUCT(std::move(child_types));
}

} // namespace

AggregateFunction BoolOrFun::GetFunction() {
	auto fun = AggregateFunction::UnaryAggregate<BoolState, bool, bool, BoolOrFunFunction>(
	    LogicalType(LogicalTypeId::BOOLEAN), LogicalType::BOOLEAN);
	fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
	fun.SetDistinctDependent(AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT);
	return fun.SetStructStateExport(GetBoolAndStateType);
}

AggregateFunction BoolAndFun::GetFunction() {
	auto fun = AggregateFunction::UnaryAggregate<BoolState, bool, bool, BoolAndFunFunction>(
	    LogicalType(LogicalTypeId::BOOLEAN), LogicalType::BOOLEAN);
	fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
	fun.SetDistinctDependent(AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT);
	return fun.SetStructStateExport(GetBoolAndStateType);
}

} // namespace duckdb
