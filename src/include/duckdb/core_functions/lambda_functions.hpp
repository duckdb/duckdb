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

#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

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
	//! Returns the parameter type for ternary lambdas
	static LogicalType BindTernaryLambda(const idx_t parameter_idx, const LogicalType &list_child_type);

	//! Checks for NULL list parameter and prepared statements and adds bound cast expression
	static unique_ptr<FunctionData> ListLambdaPrepareBind(vector<unique_ptr<Expression>> &arguments,
	                                                      ClientContext &context, ScalarFunction &bound_function);

	//! Returns the ListLambdaBindData containing the lambda expression
	static unique_ptr<FunctionData> ListLambdaBind(ClientContext &, ScalarFunction &bound_function,
	                                               vector<unique_ptr<Expression>> &arguments,
	                                               const bool has_index = false);

	//! Internally executes list_transform
	static void ListTransformFunction(DataChunk &args, ExpressionState &state, Vector &result);
	//! Internally executes list_filter
	static void ListFilterFunction(DataChunk &args, ExpressionState &state, Vector &result);
	//! Internally executes list_reduce
	static void ListReduceFunction(DataChunk &args, ExpressionState &state, Vector &result);

public:
	//! Lambda expressions can only be executed on one STANDARD_VECTOR_SIZE list child elements at a time, so for
	//! list_transform and list_filter we need to prepare the input vectors for the lambda expression.  In list_reduce
	//! the input size can never exceed row_count so it doesn't need ColumnInfo.
	struct ColumnInfo {
		explicit ColumnInfo(Vector &vector) : vector(vector), sel(SelectionVector(STANDARD_VECTOR_SIZE)) {};

		//! The original vector taken from args
		reference<Vector> vector;
		//! The selection vector to slice the original vector
		SelectionVector sel;
		//! The unified vector format of the original vector
		UnifiedVectorFormat format;
	};

	//! LambdaInfo sets up and stores the information needed by all lambda functions
	struct LambdaInfo {
		explicit LambdaInfo(DataChunk &args, ExpressionState &state, Vector &result, bool &result_is_null)
		    : result(result), row_count(args.size()), is_all_constant(args.AllConstant()) {
			Vector &list_column = args.data[0];

			result.SetVectorType(VectorType::FLAT_VECTOR);
			result_validity = &FlatVector::Validity(result);

			if (list_column.GetType().id() == LogicalTypeId::SQLNULL) {
				result.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result, true);
				result_is_null = true;
				return;
			}

			// get the lambda expression
			auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
			auto &bind_info = func_expr.bind_info->Cast<ListLambdaBindData>();
			lambda_expr = bind_info.lambda_expr;
			is_volatile = lambda_expr->IsVolatile();
			has_index = bind_info.has_index;

			// get the list column entries
			list_column.ToUnifiedFormat(row_count, list_column_format);
			list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_column_format);

			child_vector = &ListVector::GetEntry(list_column);

			// get the lambda column data for all other input vectors
			column_infos = LambdaFunctions::GetColumnInfo(args, row_count);
		};

		const list_entry_t *list_entries;
		UnifiedVectorFormat list_column_format;
		optional_ptr<Vector> child_vector;
		Vector &result;
		optional_ptr<ValidityMask> result_validity;
		vector<ColumnInfo> column_infos;
		optional_ptr<Expression> lambda_expr;

		const idx_t row_count;
		bool has_index;
		bool is_volatile;
		const bool is_all_constant;
	};

	static vector<ColumnInfo> GetColumnInfo(DataChunk &args, const idx_t row_count);
	static vector<reference<ColumnInfo>> GetInconstantColumnInfo(vector<ColumnInfo> &data);
};

} // namespace duckdb
