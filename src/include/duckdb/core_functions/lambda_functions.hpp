//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/core_functions/lambda_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

struct ListLambdaBindData : public FunctionData {
public:
	ListLambdaBindData(const LogicalType &return_type, unique_ptr<Expression> lambda_expr, const bool has_index = false)
	    : return_type(return_type), lambda_expr(std::move(lambda_expr)), has_index(has_index) {};

	//! Return type of the scalar function
	LogicalType return_type;
	//! Lambda expression that the expression executor executes
	unique_ptr<Expression> lambda_expr;
	//! True, if the last parameter in a lambda parameter list represents the index of the current list element
	bool has_index;

public:
	bool Equals(const FunctionData &other_p) const override;
	unique_ptr<FunctionData> Copy() const override;

	//! Serializes a lambda function's bind data
	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const ScalarFunction &function);
	//! Deserializes a lambda function's bind data
	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, ScalarFunction &);
};

class LambdaFunctions {
public:
	//! Returns the parameter type for binary lambdas
	static LogicalType BindBinaryLambda(const idx_t parameter_idx, const LogicalType &list_child_type);
	//! Returns the ListLambdaBindData containing the lambda expression
	static unique_ptr<FunctionData> ListLambdaBind(ClientContext &, ScalarFunction &bound_function,
	                                               vector<unique_ptr<Expression>> &arguments,
	                                               const bool has_index = false);

	//! Internally executes list_transform
	static void ListTransformFunction(DataChunk &args, ExpressionState &state, Vector &result);
	//! Internally executes list_filter
	static void ListFilterFunction(DataChunk &args, ExpressionState &state, Vector &result);
};

} // namespace duckdb
