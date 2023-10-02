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

namespace duckdb {

//! The FunctionBinder class is responsible for binding functions
class FunctionBinder {
public:
	DUCKDB_API explicit FunctionBinder(ClientContext &context);

	ClientContext &context;

public:
	//! Bind a scalar function from the set of functions and input arguments. Returns the index of the chosen function,
	//! returns DConstants::INVALID_INDEX and sets error if none could be found
	DUCKDB_API idx_t BindFunction(const string &name, ScalarFunctionSet &functions,
	                              const vector<LogicalType> &arguments, string &error);
	DUCKDB_API idx_t BindFunction(const string &name, ScalarFunctionSet &functions,
	                              vector<unique_ptr<Expression>> &arguments, string &error);
	//! Bind an aggregate function from the set of functions and input arguments. Returns the index of the chosen
	//! function, returns DConstants::INVALID_INDEX and sets error if none could be found
	DUCKDB_API idx_t BindFunction(const string &name, AggregateFunctionSet &functions,
	                              const vector<LogicalType> &arguments, string &error);
	DUCKDB_API idx_t BindFunction(const string &name, AggregateFunctionSet &functions,
	                              vector<unique_ptr<Expression>> &arguments, string &error);
	//! Bind a table function from the set of functions and input arguments. Returns the index of the chosen
	//! function, returns DConstants::INVALID_INDEX and sets error if none could be found
	DUCKDB_API idx_t BindFunction(const string &name, TableFunctionSet &functions, const vector<LogicalType> &arguments,
	                              string &error);
	DUCKDB_API idx_t BindFunction(const string &name, TableFunctionSet &functions,
	                              vector<unique_ptr<Expression>> &arguments, string &error);
	//! Bind a pragma function from the set of functions and input arguments
	DUCKDB_API idx_t BindFunction(const string &name, PragmaFunctionSet &functions, PragmaInfo &info, string &error);

	DUCKDB_API unique_ptr<Expression> BindScalarFunction(const string &schema, const string &name,
	                                                     vector<unique_ptr<Expression>> children, string &error,
	                                                     bool is_operator = false, Binder *binder = nullptr);
	DUCKDB_API unique_ptr<Expression> BindScalarFunction(ScalarFunctionCatalogEntry &function,
	                                                     vector<unique_ptr<Expression>> children, string &error,
	                                                     bool is_operator = false, Binder *binder = nullptr);

	DUCKDB_API unique_ptr<BoundFunctionExpression> BindScalarFunction(ScalarFunction bound_function,
	                                                                  vector<unique_ptr<Expression>> children,
	                                                                  bool is_operator = false);

	DUCKDB_API unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(AggregateFunction bound_function, vector<unique_ptr<Expression>> children,
	                      unique_ptr<Expression> filter = nullptr,
	                      AggregateType aggr_type = AggregateType::NON_DISTINCT);

	DUCKDB_API static void BindSortedAggregate(ClientContext &context, BoundAggregateExpression &expr,
	                                           const vector<unique_ptr<Expression>> &groups);

private:
	//! Cast a set of expressions to the arguments of this function
	void CastToFunctionArguments(SimpleFunction &function, vector<unique_ptr<Expression>> &children);
	int64_t BindVarArgsFunctionCost(const SimpleFunction &func, const vector<LogicalType> &arguments);
	int64_t BindFunctionCost(const SimpleFunction &func, const vector<LogicalType> &arguments);

	template <class T>
	vector<idx_t> BindFunctionsFromArguments(const string &name, FunctionSet<T> &functions,
	                                         const vector<LogicalType> &arguments, string &error);

	template <class T>
	idx_t MultipleCandidateException(const string &name, FunctionSet<T> &functions, vector<idx_t> &candidate_functions,
	                                 const vector<LogicalType> &arguments, string &error);

	template <class T>
	idx_t BindFunctionFromArguments(const string &name, FunctionSet<T> &functions, const vector<LogicalType> &arguments,
	                                string &error);

	vector<LogicalType> GetLogicalTypesFromExpressions(vector<unique_ptr<Expression>> &arguments);
};

} // namespace duckdb
