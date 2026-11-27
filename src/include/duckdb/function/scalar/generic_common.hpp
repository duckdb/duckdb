//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/generic_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {
class BoundAggregateFunction;
class BoundFunctionExpression;
struct AggregateStateLayout;
struct AggregateStateSerializer;
struct SortedAggregateStateOrder;

struct ConstantOrNull {
	static bool IsConstantOrNull(BoundFunctionExpression &expr, const Value &val);
};

struct ExportAggregateFunctionBindData : public FunctionData {
	unique_ptr<BoundAggregateExpression> aggregate;
	explicit ExportAggregateFunctionBindData(unique_ptr<Expression> aggregate_p);
	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};

struct ExportAggregateFunction {
	static unique_ptr<BoundAggregateExpression> Bind(unique_ptr<BoundAggregateExpression> child_aggregate);
	static void SetStateExport(BoundAggregateExpression &aggregate, LogicalType state_layout);
	static unique_ptr<ParsedExpression> StateToSQL(const LogicalType &type, unique_ptr<ParsedExpression> value);
	//! The per-key (column, modifiers) of the AGGREGATE_STATE type of an ordered aggregate
	static void GetSortedStateOrders(const LogicalType &state_type, vector<SortedAggregateStateOrder> &orders);
	//! The serializer of the aggregate whose states a bound combine_aggr accumulates. Returns nullptr for any other
	//! function
	static unique_ptr<AggregateStateSerializer>
	TryGetCombinedStateSerializer(const BoundAggregateFunction &function, optional_ptr<const FunctionData> bind_data);
};

} // namespace duckdb
