//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/window_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/error_data.hpp"

namespace duckdb {

class WindowFunctionCatalogEntry;

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
	DUCKDB_API optional_idx BindFunction(const Identifier &name, const ScalarFunctionSet &functions,
	                                     const vector<LogicalType> &regular_args,
	                                     const vector<pair<Identifier, LogicalType>> &keyword_args, ErrorData &error);
	DUCKDB_API optional_idx BindFunction(const Identifier &name, const ScalarFunctionSet &functions,
	                                     const vector<LogicalType> &regular_args, ErrorData &error) {
		return BindFunctionFromArguments(name, functions, regular_args, {}, error);
	}

	DUCKDB_API optional_idx BindFunction(const Identifier &name, const ScalarFunctionSet &functions,
	                                     const vector<unique_ptr<Expression>> &regular_args,
	                                     const vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args,
	                                     ErrorData &error);

	//! Bind an aggregate function from the set of functions and input arguments. Returns the index of the chosen
	//! function, returns optional_idx() and sets error if none could be found
	DUCKDB_API optional_idx BindFunction(const Identifier &name, const AggregateFunctionSet &functions,
	                                     const vector<LogicalType> &regular_args,
	                                     const vector<pair<Identifier, LogicalType>> &keyword_args, ErrorData &error);
	DUCKDB_API optional_idx BindFunction(const Identifier &name, const AggregateFunctionSet &functions,
	                                     const vector<LogicalType> &regular_args, ErrorData &error) {
		return BindFunctionFromArguments(name, functions, regular_args, {}, error);
	}

	DUCKDB_API optional_idx BindFunction(const Identifier &name, const AggregateFunctionSet &functions,
	                                     const vector<unique_ptr<Expression>> &regular_args,
	                                     const vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args,
	                                     ErrorData &error);

	//! Bind an aggregate function from the set of functions and input arguments. Returns the index of the chosen
	//! function, returns optional_idx() and sets error if none could be found
	DUCKDB_API optional_idx BindFunction(const Identifier &name, const WindowFunctionSet &functions,
	                                     const vector<LogicalType> &regular_args,
	                                     const vector<pair<Identifier, LogicalType>> &keyword_args, ErrorData &error);

	DUCKDB_API optional_idx BindFunction(const Identifier &name, const WindowFunctionSet &functions,
	                                     const vector<LogicalType> &regular_args, ErrorData &error) {
		return BindFunctionFromArguments(name, functions, regular_args, {}, error);
	}

	//! Bind a table function from the set of functions and input arguments. Returns the index of the chosen
	//! function, returns optional_idx() and sets error if none could be found
	DUCKDB_API optional_idx BindFunction(const Identifier &name, const TableFunctionSet &functions,
	                                     const vector<LogicalType> &regular_args,
	                                     const vector<pair<Identifier, LogicalType>> &keyword_args, ErrorData &error);
	DUCKDB_API optional_idx BindFunction(const Identifier &name, const TableFunctionSet &functions,
	                                     const vector<LogicalType> &regular_args, ErrorData &error) {
		return BindFunctionFromArguments(name, functions, regular_args, {}, error);
	}

	DUCKDB_API optional_idx BindFunction(const Identifier &name, const TableFunctionSet &functions,
	                                     const vector<unique_ptr<Expression>> &regular_args,
	                                     const vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args,
	                                     ErrorData &error);

	//! Bind a pragma function from the set of functions and input arguments
	DUCKDB_API optional_idx BindFunction(const Identifier &name, const PragmaFunctionSet &functions,
	                                     vector<Value> &parameters, ErrorData &error);

	DUCKDB_API unique_ptr<Expression> BindScalarFunction(const Identifier &schema, const Identifier &name,
	                                                     vector<unique_ptr<Expression>> children, ErrorData &error,
	                                                     bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr);

	DUCKDB_API unique_ptr<Expression> BindScalarFunction(const ScalarFunctionCatalogEntry &function,
	                                                     vector<unique_ptr<Expression>> children, ErrorData &error,
	                                                     bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr);

	DUCKDB_API unique_ptr<Expression> BindScalarFunction(const ScalarFunction &bound_function,
	                                                     vector<unique_ptr<Expression>> children,
	                                                     bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr);

	//! Bind a scalar function from a catalog entry given the full list of (maybe-named) bound arguments. The
	//! positional/named split is resolved per candidate overload (overloads flagged to capture argument aliases
	//! treat every argument as positional and keep its alias).
	DUCKDB_API unique_ptr<Expression> BindScalarFunction(const ScalarFunctionCatalogEntry &function,
	                                                     vector<pair<Identifier, unique_ptr<Expression>>> arguments,
	                                                     ErrorData &error, bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr);

	DUCKDB_API unique_ptr<Expression> BindScalarFunction(const ScalarFunction &bound_function,
	                                                     vector<unique_ptr<Expression>> children,
	                                                     vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
	                                                     bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr);

	DUCKDB_API unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(const AggregateFunction &bound_function, vector<unique_ptr<Expression>> children,
	                      unique_ptr<Expression> filter = nullptr,
	                      AggregateType aggr_type = AggregateType::NON_DISTINCT);

	DUCKDB_API unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(const AggregateFunction &function, vector<unique_ptr<Expression>> children,
	                      vector<pair<Identifier, unique_ptr<Expression>>> keyword_args, unique_ptr<Expression> filter,
	                      AggregateType aggr_type);

	DUCKDB_API unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(const AggregateFunctionCatalogEntry &function,
	                      vector<pair<Identifier, unique_ptr<Expression>>> arguments, ErrorData &error,
	                      unique_ptr<Expression> filter = nullptr,
	                      AggregateType aggr_type = AggregateType::NON_DISTINCT);

	DUCKDB_API static void BindSortedAggregate(ClientContext &context, BoundAggregateExpression &expr,
	                                           const vector<unique_ptr<Expression>> &groups,
	                                           optional_ptr<vector<GroupingSet>> grouping_sets);
	DUCKDB_API static void BindSortedAggregate(ClientContext &context, BoundWindowExpression &expr);

	DUCKDB_API unique_ptr<BoundWindowExpression>
	BindWindowFunction(const WindowFunction &function, vector<unique_ptr<Expression>> children,
	                   vector<pair<Identifier, unique_ptr<Expression>>> keyword_args, vector<OrderByNode> &orders,
	                   vector<OrderByNode> &arg_orders);

	DUCKDB_API unique_ptr<BoundWindowExpression> BindWindowFunction(const WindowFunction &function,
	                                                                vector<unique_ptr<Expression>> children,
	                                                                vector<OrderByNode> &orders,
	                                                                vector<OrderByNode> &arg_orders);

	DUCKDB_API unique_ptr<BoundWindowExpression>
	BindWindowFunction(const WindowFunctionCatalogEntry &function,
	                   vector<pair<Identifier, unique_ptr<Expression>>> arguments, ErrorData &error,
	                   vector<OrderByNode> &orders, vector<OrderByNode> &arg_orders);

	pair<BoundScalarFunction, unique_ptr<FunctionData>> ResolveFunction(const ScalarFunction &function,
	                                                                    vector<unique_ptr<Expression>> &children) {
		vector<pair<Identifier, unique_ptr<Expression>>> empty_keyword_args;
		return ResolveFunction(function, children, empty_keyword_args);
	}

	pair<BoundScalarFunction, unique_ptr<FunctionData>>
	ResolveFunction(const ScalarFunction &function, vector<unique_ptr<Expression>> &children,
	                vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args);

	pair<BoundAggregateFunction, unique_ptr<FunctionData>>
	ResolveFunction(const AggregateFunction &function, vector<unique_ptr<Expression>> &children,
	                vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args);

	pair<BoundAggregateFunction, unique_ptr<FunctionData>> ResolveFunction(const AggregateFunction &function,
	                                                                       vector<unique_ptr<Expression>> &children) {
		vector<pair<Identifier, unique_ptr<Expression>>> empty_keyword_args;
		return ResolveFunction(function, children, empty_keyword_args);
	}

	pair<BoundWindowFunction, unique_ptr<FunctionData>>
	ResolveFunction(const WindowFunction &function, vector<unique_ptr<Expression>> &children,
	                vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args,
	                optional_ptr<vector<OrderByNode>> orders = nullptr,
	                optional_ptr<vector<OrderByNode>> arg_orders = nullptr);

	pair<BoundWindowFunction, unique_ptr<FunctionData>> ResolveFunction(const WindowFunction &function,
	                                                                    vector<unique_ptr<Expression>> &children) {
		vector<pair<Identifier, unique_ptr<Expression>>> empty_keyword_args;
		return ResolveFunction(function, children, empty_keyword_args);
	}

private:
	//! Cast a set of expressions to the arguments of this function
	void CastToFunctionArguments(BoundSimpleFunction &function, vector<unique_ptr<Expression>> &children);

	void ResolveTemplateTypes(BoundSimpleFunction &bound_function, const vector<unique_ptr<Expression>> &children);
	void CheckTemplateTypesResolved(const BoundSimpleFunction &bound_function);

	optional_idx BindFunctionCost(const SimpleFunction &func, const vector<LogicalType> &arguments,
	                              const vector<pair<Identifier, LogicalType>> &named_arguments);

	optional_idx BindVarArgsFunctionCost(const SimpleNamedParameterFunction &func,
	                                     const vector<LogicalType> &arguments);
	optional_idx BindFunctionCost(const SimpleNamedParameterFunction &func, const vector<LogicalType> &arguments,
	                              const vector<pair<Identifier, LogicalType>> &);

	template <class T>
	vector<idx_t> BindFunctionsFromArguments(const Identifier &name, const FunctionSet<T> &functions,
	                                         const vector<LogicalType> &arguments,
	                                         const vector<pair<Identifier, LogicalType>> &named_arguments,
	                                         ErrorData &error);

	template <class T>
	optional_idx BindFunctionFromArguments(const Identifier &name, const FunctionSet<T> &functions,
	                                       const vector<LogicalType> &arguments,
	                                       const vector<pair<Identifier, LogicalType>> &named_arguments,
	                                       ErrorData &error);

	//! Select the best matching overload for the given full (maybe-named) argument list.
	template <class T>
	optional_idx BindFunctionFromArguments(const Identifier &name, const FunctionSet<T> &functions,
	                                       vector<pair<Identifier, unique_ptr<Expression>>> &arguments,
	                                       ErrorData &error);

	pair<vector<LogicalType>, vector<pair<Identifier, LogicalType>>>
	GetArgumentsFromExpressions(const vector<unique_ptr<Expression>> &regular_arguments,
	                            const vector<pair<Identifier, unique_ptr<Expression>>> &keyword_arguments);
};

} // namespace duckdb
