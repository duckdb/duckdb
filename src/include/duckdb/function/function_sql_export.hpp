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
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/result_modifier.hpp"

namespace duckdb {

class BoundAggregateFunction;
class BoundScalarFunction;
struct FunctionData;

class FunctionSQLExportResult {
public:
	static FunctionSQLExportResult Success(unique_ptr<ParsedExpression> expression) {
		return FunctionSQLExportResult(true, std::move(expression), string());
	}

	static FunctionSQLExportResult Failure(string message) {
		return FunctionSQLExportResult(false, nullptr, std::move(message));
	}

	bool IsSuccess() const {
		return success;
	}
	bool HasError() const {
		return !success;
	}
	bool IsValid() const {
		return success ? expression != nullptr && error.empty() : expression == nullptr && !error.empty();
	}
	unique_ptr<ParsedExpression> &GetValue() {
		D_ASSERT(IsSuccess());
		return expression;
	}
	const unique_ptr<ParsedExpression> &GetValue() const {
		D_ASSERT(IsSuccess());
		return expression;
	}
	const string &GetError() const {
		D_ASSERT(HasError());
		return error;
	}

private:
	FunctionSQLExportResult(bool success_p, unique_ptr<ParsedExpression> expression_p, string error_p)
	    : success(success_p), expression(std::move(expression_p)), error(std::move(error_p)) {
	}

private:
	bool success;
	unique_ptr<ParsedExpression> expression;
	string error;
};

struct ScalarFunctionSQLExportInput {
	ScalarFunctionSQLExportInput(const BoundScalarFunction &function_p, optional_ptr<const FunctionData> bind_data_p,
	                             vector<unique_ptr<ParsedExpression>> children_p)
	    : function(function_p), bind_data(bind_data_p), children(std::move(children_p)) {
	}

	//! Temporary references are valid only for the duration of the callback and must not be retained.
	const BoundScalarFunction &function;
	optional_ptr<const FunctionData> bind_data;
	vector<unique_ptr<ParsedExpression>> children;
};

struct AggregateFunctionSQLExportInput {
	AggregateFunctionSQLExportInput(const BoundAggregateFunction &function_p,
	                                optional_ptr<const FunctionData> bind_data_p,
	                                vector<unique_ptr<ParsedExpression>> children_p,
	                                unique_ptr<ParsedExpression> filter_p, unique_ptr<OrderModifier> order_bys_p,
	                                AggregateType aggregate_type_p, AggregateStateExportMode state_export_mode_p)
	    : function(function_p), bind_data(bind_data_p), children(std::move(children_p)), filter(std::move(filter_p)),
	      order_bys(std::move(order_bys_p)), aggregate_type(aggregate_type_p), state_export_mode(state_export_mode_p) {
	}

	//! Temporary references are valid only for the duration of the callback and must not be retained.
	const BoundAggregateFunction &function;
	optional_ptr<const FunctionData> bind_data;
	vector<unique_ptr<ParsedExpression>> children;
	unique_ptr<ParsedExpression> filter;
	unique_ptr<OrderModifier> order_bys;
	AggregateType aggregate_type;
	AggregateStateExportMode state_export_mode;
};

using scalar_function_sql_export_t = FunctionSQLExportResult (*)(ScalarFunctionSQLExportInput &input);
using aggregate_function_sql_export_t = FunctionSQLExportResult (*)(AggregateFunctionSQLExportInput &input);

} // namespace duckdb
