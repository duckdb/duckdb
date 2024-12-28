//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/error_data.hpp"

namespace duckdb {

//! The FunctionBinder class is responsible for binding functions
class FunctionBinder {
public:
	DUCKDB_API explicit FunctionBinder(Binder &binder);
	DUCKDB_API explicit FunctionBinder(ClientContext &context);

	optional_ptr<Binder> binder;
	ClientContext &context;

public:
	//! Bind a scalar function from the set of functions and input arguments. Returns the index of the chosen function,
	//! returns optional_idx() and sets error if none could be found
	DUCKDB_API optional_idx BindFunction(const string &name, ScalarFunctionSet &functions,
	                                     const vector<LogicalType> &arguments, ErrorData &error);
	DUCKDB_API optional_idx BindFunction(const string &name, ScalarFunctionSet &functions,
	                                     vector<unique_ptr<Expression>> &arguments, ErrorData &error);
	//! Bind an aggregate function from the set of functions and input arguments. Returns the index of the chosen
	//! function, returns optional_idx() and sets error if none could be found
	DUCKDB_API optional_idx BindFunction(const string &name, AggregateFunctionSet &functions,
	                                     const vector<LogicalType> &arguments, ErrorData &error);
	DUCKDB_API optional_idx BindFunction(const string &name, AggregateFunctionSet &functions,
	                                     vector<unique_ptr<Expression>> &arguments, ErrorData &error);
	//! Bind a table function from the set of functions and input arguments. Returns the index of the chosen
	//! function, returns optional_idx() and sets error if none could be found
	DUCKDB_API optional_idx BindFunction(const string &name, TableFunctionSet &functions,
	                                     const vector<LogicalType> &arguments, ErrorData &error);
	DUCKDB_API optional_idx BindFunction(const string &name, TableFunctionSet &functions,
	                                     vector<unique_ptr<Expression>> &arguments, ErrorData &error);
	//! Bind a pragma function from the set of functions and input arguments
	DUCKDB_API optional_idx BindFunction(const string &name, PragmaFunctionSet &functions, vector<Value> &parameters,
	                                     ErrorData &error);

	DUCKDB_API unique_ptr<Expression> BindScalarFunction(const string &schema, const string &name,
	                                                     vector<unique_ptr<Expression>> children, ErrorData &error,
	                                                     bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr);
	DUCKDB_API unique_ptr<Expression> BindScalarFunction(ScalarFunctionCatalogEntry &function,
	                                                     vector<unique_ptr<Expression>> children, ErrorData &error,
	                                                     bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr);

	DUCKDB_API unique_ptr<Expression> BindScalarFunction(ScalarFunction bound_function,
	                                                     vector<unique_ptr<Expression>> children,
	                                                     bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr);

	DUCKDB_API unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(AggregateFunction bound_function, vector<unique_ptr<Expression>> children,
	                      unique_ptr<Expression> filter = nullptr,
	                      AggregateType aggr_type = AggregateType::NON_DISTINCT);

	DUCKDB_API static void BindSortedAggregate(ClientContext &context, BoundAggregateExpression &expr,
	                                           const vector<unique_ptr<Expression>> &groups);
	DUCKDB_API static void BindSortedAggregate(ClientContext &context, BoundWindowExpression &expr);

private:
	//! Cast a set of expressions to the arguments of this function
	void CastToFunctionArguments(SimpleFunction &function, vector<unique_ptr<Expression>> &children);
	optional_idx BindVarArgsFunctionCost(const SimpleFunction &func, const vector<LogicalType> &arguments);
	optional_idx BindFunctionCost(const SimpleFunction &func, const vector<LogicalType> &arguments);

	template <class T>
	vector<idx_t> BindFunctionsFromArguments(const string &name, FunctionSet<T> &functions,
	                                         const vector<LogicalType> &arguments, ErrorData &error);

	template <class T>
	optional_idx MultipleCandidateException(const string &name, FunctionSet<T> &functions,
	                                        vector<idx_t> &candidate_functions, const vector<LogicalType> &arguments,
	                                        ErrorData &error);

	template <class T>
	optional_idx BindFunctionFromArguments(const string &name, FunctionSet<T> &functions,
	                                       const vector<LogicalType> &arguments, ErrorData &error);

	vector<LogicalType> GetLogicalTypesFromExpressions(vector<unique_ptr<Expression>> &arguments);
};

} // namespace duckdb
