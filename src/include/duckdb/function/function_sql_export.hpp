//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function_sql_export.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/planner/logical_plan_verification_result.hpp"

namespace duckdb {

class BoundAggregateFunction;
class BoundScalarFunction;
struct FunctionData;

using FunctionSQLExportResult = LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>;

struct ScalarFunctionSQLExportInput {
	ScalarFunctionSQLExportInput(const BoundScalarFunction &function_p, optional_ptr<const FunctionData> bind_data_p,
	                             vector<unique_ptr<ParsedExpression>> children_p, LogicalPlanVerificationPath path_p,
	                             LogicalPlanVerificationFunctionIdentity identity_p)
	    : function(function_p), bind_data(bind_data_p), children(std::move(children_p)), path(std::move(path_p)),
	      identity(std::move(identity_p)) {
	}

	const BoundScalarFunction &function;
	optional_ptr<const FunctionData> bind_data;
	vector<unique_ptr<ParsedExpression>> children;
	LogicalPlanVerificationPath path;
	LogicalPlanVerificationFunctionIdentity identity;
};

struct AggregateFunctionSQLExportInput {
	AggregateFunctionSQLExportInput(const BoundAggregateFunction &function_p,
	                                optional_ptr<const FunctionData> bind_data_p,
	                                vector<unique_ptr<ParsedExpression>> children_p,
	                                unique_ptr<ParsedExpression> filter_p, unique_ptr<OrderModifier> order_bys_p,
	                                AggregateType aggregate_type_p, AggregateStateExportMode state_export_mode_p,
	                                LogicalPlanVerificationPath path_p,
	                                LogicalPlanVerificationFunctionIdentity identity_p)
	    : function(function_p), bind_data(bind_data_p), children(std::move(children_p)), filter(std::move(filter_p)),
	      order_bys(std::move(order_bys_p)), aggregate_type(aggregate_type_p), state_export_mode(state_export_mode_p),
	      path(std::move(path_p)), identity(std::move(identity_p)) {
	}

	const BoundAggregateFunction &function;
	optional_ptr<const FunctionData> bind_data;
	vector<unique_ptr<ParsedExpression>> children;
	unique_ptr<ParsedExpression> filter;
	unique_ptr<OrderModifier> order_bys;
	AggregateType aggregate_type;
	AggregateStateExportMode state_export_mode;
	LogicalPlanVerificationPath path;
	LogicalPlanVerificationFunctionIdentity identity;
};

using scalar_function_sql_export_t = FunctionSQLExportResult (*)(ScalarFunctionSQLExportInput &input);
using aggregate_function_sql_export_t = FunctionSQLExportResult (*)(AggregateFunctionSQLExportInput &input);

} // namespace duckdb
