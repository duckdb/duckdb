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
	static LogicalType BindTertiaryLambda(const idx_t parameter_idx, const LogicalType &list_child_type);
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
	//! LambdaColumnInfo holds information for preparing the input vectors. We prepare the input vectors
	//! for executing a lambda expression on STANDARD_VECTOR_SIZE list child elements at a time.
	struct ColumnInfo {
		explicit ColumnInfo(Vector &vector) : vector(vector), sel(SelectionVector(STANDARD_VECTOR_SIZE)) {};

		//! The original vector taken from args
		reference<Vector> vector;
		//! The selection vector to slice the original vector
		SelectionVector sel;
		//! The unified vector format of the original vector
		UnifiedVectorFormat format;
	};

	struct LambdaInfo {
		explicit LambdaInfo(DataChunk &args) : row_count(args.size()), is_all_constant(args.AllConstant()) {};

		const idx_t row_count;
		bool has_index;
		bool has_side_effects;
		const bool is_all_constant;
	};

	struct ReduceInfo {
		explicit ReduceInfo(const list_entry_t *list_entries, const UnifiedVectorFormat &list_column_format, Vector &child_vector,
		                    Vector &result, const vector<ColumnInfo> &column_infos)
		    : list_entries(list_entries), list_column_format(list_column_format), child_vector(child_vector),
		      result(result), column_infos(column_infos) {};

		const list_entry_t *list_entries;
		const UnifiedVectorFormat &list_column_format;
		Vector &child_vector;
		Vector &result;
		const vector<ColumnInfo> &column_infos;
	};

	static void PrepareReduce(unique_ptr<Expression> &lambda_expr, ValidityMask &result_validity, LambdaInfo &info, ReduceInfo &reduce_info, ExpressionState &state);

	static vector<ColumnInfo> GetColumnInfo(DataChunk &args, const idx_t row_count);
	static vector<reference<ColumnInfo>> GetInconstantColumnInfo(vector<ColumnInfo> &data);
};

} // namespace duckdb
