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
#include "duckdb/common/enums/order_type.hpp"

namespace duckdb {
class AggregateFunctionCatalogEntry;

class WindowFunctionCatalogEntry;

//! One ORDER BY key of an exported ordered aggregate state: the buffered struct column it sorts on and the modifiers.
struct SortedAggregateStateOrder {
	idx_t column;
	OrderType order_type;
	OrderByNullType null_order;
};

//! A scalar function with its argument and return types resolved, before the bind callback is invoked
struct ResolvedScalarFunctionTypes {
	BoundScalarFunction bound_function;
	vector<Identifier> argument_names;
	//! The collation pushed into the arguments, to be propagated to the return type after binding
	string pushed_collation;
};

//! The overload a call selects out of a function set, and how the arguments of the call are passed to it
struct FunctionBinding {
	//! Whether an overload was selected - if not, the error of the selection says why
	bool IsValid() const {
		return function_index.IsValid();
	}
	//! The index of the selected overload in the function set
	idx_t GetIndex() const {
		return function_index.GetIndex();
	}

	//! The index of the selected overload in the function set - invalid if no overload matches the call
	optional_idx function_index;
	//! The cost of casting the arguments to the parameters of the selected overload
	idx_t cost = 0;
	//! The parameter of the overload that receives each argument - the positional arguments, followed by the named
	//! ones. Arguments received by "*args" or "**kwargs" map to that parameter
	vector<idx_t> argument_parameters;
	//! The type each argument is passed as: the type of its parameter, with the templates the argument types
	//! determine substituted
	vector<LogicalType> parameter_types;
};

//! The FunctionBinder class is responsible for binding functions
class FunctionBinder {
public:
	DUCKDB_API explicit FunctionBinder(Binder &binder);
	DUCKDB_API explicit FunctionBinder(ClientContext &context);

	optional_ptr<Binder> binder;
	ClientContext &context;

public:
	//! Overload selection: the cheapest overload of a function set that a call with the given argument types binds
	//! to. Selection needs no binder - without a ClientContext only the built-in implicit casts are considered.
	//! Invalid, with the error set, if no overload matches or the choice is ambiguous
	DUCKDB_API static FunctionBinding SelectFunction(optional_ptr<ClientContext> context, const Identifier &name,
	                                                 const BaseFunctionSet &functions,
	                                                 const vector<LogicalType> &arguments,
	                                                 const vector<pair<Identifier, LogicalType>> &named_arguments,
	                                                 ErrorData &error);
	static FunctionBinding SelectFunction(optional_ptr<ClientContext> context, const Identifier &name,
	                                      const BaseFunctionSet &functions, const vector<LogicalType> &arguments,
	                                      ErrorData &error) {
		return SelectFunction(context, name, functions, arguments, {}, error);
	}
	//! Selects by the return types of the bound arguments
	DUCKDB_API static FunctionBinding
	SelectFunction(optional_ptr<ClientContext> context, const Identifier &name, const BaseFunctionSet &functions,
	               const vector<unique_ptr<Expression>> &arguments,
	               const vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments, ErrorData &error);
	//! All overloads that match at the lowest cost. Empty, with the error set, if none match
	DUCKDB_API static vector<idx_t> Candidates(optional_ptr<ClientContext> context, const Identifier &name,
	                                           const BaseFunctionSet &functions, const vector<LogicalType> &arguments,
	                                           const vector<pair<Identifier, LogicalType>> &named_arguments,
	                                           ErrorData &error);
	//! Cost of calling an overload with the given argument types, or an invalid index if it does not match
	DUCKDB_API static optional_idx Cost(optional_ptr<ClientContext> context, const SimpleFunction &function,
	                                    const vector<LogicalType> &arguments,
	                                    const vector<pair<Identifier, LogicalType>> &named_arguments);

	//! Bind a table function: select the overload, check the named arguments against its signature, and cast both
	//! the positional and the named arguments to the types it declares
	DUCKDB_API FunctionBinding BindFunctionArguments(const Identifier &name, const TableFunctionSet &functions,
	                                                 vector<unique_ptr<Expression>> &positional_arguments,
	                                                 vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments,
	                                                 vector<Value> &parameters, named_argument_map_t &named_parameters,
	                                                 ErrorData &error);

	//! Casts a constant to the type of the parameter it fills, as an argument of any function is cast - a parameter
	//! whose type is not concrete, such as ANY, takes it as it is
	DUCKDB_API static Value CastToParameterType(ClientContext &context, Value value, const LogicalType &parameter_type);

	//! Bind a pragma function from the set of functions and input arguments
	DUCKDB_API FunctionBinding BindFunctionArguments(const Identifier &name, const PragmaFunctionSet &functions,
	                                                 vector<unique_ptr<Expression>> &positional_arguments,
	                                                 vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments,
	                                                 vector<Value> &parameters, named_argument_map_t &named_parameters,
	                                                 ErrorData &error);

	DUCKDB_API unique_ptr<Expression> BindScalarFunction(const Identifier &schema, const Identifier &name,
	                                                     vector<unique_ptr<Expression>> children, ErrorData &error,
	                                                     bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr);

	//! Throws the binding error instead of returning nullptr
	DUCKDB_API unique_ptr<Expression> BindScalarFunction(const Identifier &schema, const Identifier &name,
	                                                     vector<unique_ptr<Expression>> children,
	                                                     bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr);

	DUCKDB_API unique_ptr<Expression> BindScalarFunction(const ScalarFunctionCatalogEntry &function,
	                                                     vector<unique_ptr<Expression>> children, ErrorData &error,
	                                                     bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr);

	DUCKDB_API unique_ptr<Expression> BindScalarFunction(shared_ptr<const ScalarFunction> function,
	                                                     vector<unique_ptr<Expression>> children,
	                                                     bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr);

	//! Bind a function that does not come from a function set - the function is copied into a definition of its own
	DUCKDB_API unique_ptr<Expression> BindScalarFunction(const ScalarFunction &function,
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

	DUCKDB_API unique_ptr<Expression> BindScalarFunction(shared_ptr<const ScalarFunction> function,
	                                                     vector<unique_ptr<Expression>> children,
	                                                     vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
	                                                     bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr);

	DUCKDB_API unique_ptr<Expression> BindScalarFunction(const ScalarFunction &function,
	                                                     vector<unique_ptr<Expression>> children,
	                                                     vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
	                                                     bool is_operator = false,
	                                                     optional_ptr<Binder> binder = nullptr);

	DUCKDB_API unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(shared_ptr<const AggregateFunction> function, vector<unique_ptr<Expression>> children,
	                      unique_ptr<Expression> filter = nullptr,
	                      AggregateType aggr_type = AggregateType::NON_DISTINCT);

	//! Bind a function that does not come from a function set - the function is copied into a definition of its own
	DUCKDB_API unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(const AggregateFunction &function, vector<unique_ptr<Expression>> children,
	                      unique_ptr<Expression> filter = nullptr,
	                      AggregateType aggr_type = AggregateType::NON_DISTINCT);

	DUCKDB_API unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(shared_ptr<const AggregateFunction> function, vector<unique_ptr<Expression>> children,
	                      vector<pair<Identifier, unique_ptr<Expression>>> keyword_args, unique_ptr<Expression> filter,
	                      AggregateType aggr_type);

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

	//! Computes the exported buffer layout of an ordered aggregate: the struct of buffered columns (arguments first,
	//! then any appended sort keys), the per-key column + modifiers, and the number of leading argument columns.
	//! Mirrors the matching done by the sorted aggregate bind data so the export type matches the runtime buffer.
	DUCKDB_API static void GetSortedAggregateStateLayout(const BoundAggregateExpression &expr,
	                                                     LogicalType &buffer_struct,
	                                                     vector<SortedAggregateStateOrder> &orders,
	                                                     idx_t &argument_count);
	//! Reconstructs a sorted aggregate wrapper from an exported buffer state so finalize/combine operate on the buffer:
	//! finalize sorts by the keys and runs the (already re-bound) inner aggregate, combine concatenates buffers.
	//! Returns the wrapper function and its bind data.
	DUCKDB_API static pair<AggregateFunction, unique_ptr<FunctionData>>
	BindSortedAggregateState(ClientContext &context, const BoundAggregateFunction &inner_function,
	                         unique_ptr<FunctionData> inner_bind_info, const LogicalType &buffer_struct,
	                         const vector<SortedAggregateStateOrder> &orders, idx_t argument_count);

	DUCKDB_API unique_ptr<BoundWindowExpression>
	BindWindowFunction(shared_ptr<const WindowFunction> function, vector<unique_ptr<Expression>> children,
	                   vector<pair<Identifier, unique_ptr<Expression>>> keyword_args, vector<LogicalType> &order_types,
	                   vector<LogicalType> &arg_order_types);

	//! Bind a function that does not come from a function set - the function is copied into a definition of its own
	DUCKDB_API unique_ptr<BoundWindowExpression>
	BindWindowFunction(const WindowFunction &function, vector<unique_ptr<Expression>> children,
	                   vector<pair<Identifier, unique_ptr<Expression>>> keyword_args, vector<LogicalType> &order_types,
	                   vector<LogicalType> &arg_order_types);

	DUCKDB_API unique_ptr<BoundWindowExpression> BindWindowFunction(shared_ptr<const WindowFunction> function,
	                                                                vector<unique_ptr<Expression>> children,
	                                                                vector<LogicalType> &order_types,
	                                                                vector<LogicalType> &arg_order_types);

	DUCKDB_API unique_ptr<BoundWindowExpression> BindWindowFunction(const WindowFunction &function,
	                                                                vector<unique_ptr<Expression>> children,
	                                                                vector<LogicalType> &order_types,
	                                                                vector<LogicalType> &arg_order_types);

	DUCKDB_API unique_ptr<BoundWindowExpression>
	BindWindowFunction(const WindowFunctionCatalogEntry &function,
	                   vector<pair<Identifier, unique_ptr<Expression>>> arguments, ErrorData &error,
	                   vector<LogicalType> &order_types, vector<LogicalType> &arg_order_types);

	DUCKDB_API unique_ptr<Expression> BindScalarWindowFunction(BoundWindowExpression &wexpr,
	                                                           const vector<LogicalType> &input_types);

	//! Turn a function into a BoundScalarFunction bound to the given arguments. The function is kept as the
	//! definition of the resulting bound function - see BoundScalarFunction::GetDefinition().
	pair<BoundScalarFunction, unique_ptr<FunctionData>>
	ResolveFunction(shared_ptr<const ScalarFunction> function, vector<unique_ptr<Expression>> &children,
	                vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args);

	pair<BoundScalarFunction, unique_ptr<FunctionData>> ResolveFunction(shared_ptr<const ScalarFunction> function,
	                                                                    vector<unique_ptr<Expression>> &children) {
		vector<pair<Identifier, unique_ptr<Expression>>> empty_keyword_args;
		return ResolveFunction(std::move(function), children, empty_keyword_args);
	}

	//! Resolve a function that does not come from a function set - it is copied into a definition of its own
	pair<BoundScalarFunction, unique_ptr<FunctionData>>
	ResolveFunction(const ScalarFunction &function, vector<unique_ptr<Expression>> &children,
	                vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args) {
		return ResolveFunction(make_shared_ptr<ScalarFunction>(function), children, keyword_args);
	}

	pair<BoundScalarFunction, unique_ptr<FunctionData>> ResolveFunction(const ScalarFunction &function,
	                                                                    vector<unique_ptr<Expression>> &children) {
		return ResolveFunction(make_shared_ptr<ScalarFunction>(function), children);
	}

	pair<BoundAggregateFunction, unique_ptr<FunctionData>>
	ResolveFunction(shared_ptr<const AggregateFunction> function, vector<unique_ptr<Expression>> &children,
	                vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args);

	pair<BoundAggregateFunction, unique_ptr<FunctionData>> ResolveFunction(shared_ptr<const AggregateFunction> function,
	                                                                       vector<unique_ptr<Expression>> &children) {
		vector<pair<Identifier, unique_ptr<Expression>>> empty_keyword_args;
		return ResolveFunction(std::move(function), children, empty_keyword_args);
	}

	pair<BoundAggregateFunction, unique_ptr<FunctionData>>
	ResolveFunction(const AggregateFunction &function, vector<unique_ptr<Expression>> &children,
	                vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args) {
		return ResolveFunction(make_shared_ptr<AggregateFunction>(function), children, keyword_args);
	}

	pair<BoundAggregateFunction, unique_ptr<FunctionData>> ResolveFunction(const AggregateFunction &function,
	                                                                       vector<unique_ptr<Expression>> &children) {
		return ResolveFunction(make_shared_ptr<AggregateFunction>(function), children);
	}

	pair<BoundWindowFunction, unique_ptr<FunctionData>>
	ResolveFunction(shared_ptr<const WindowFunction> function, vector<unique_ptr<Expression>> &children,
	                vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args,
	                optional_ptr<vector<LogicalType>> order_types = nullptr,
	                optional_ptr<vector<LogicalType>> arg_order_types = nullptr);

	pair<BoundWindowFunction, unique_ptr<FunctionData>> ResolveFunction(shared_ptr<const WindowFunction> function,
	                                                                    vector<unique_ptr<Expression>> &children) {
		vector<pair<Identifier, unique_ptr<Expression>>> empty_keyword_args;
		return ResolveFunction(std::move(function), children, empty_keyword_args);
	}

	pair<BoundWindowFunction, unique_ptr<FunctionData>>
	ResolveFunction(const WindowFunction &function, vector<unique_ptr<Expression>> &children,
	                vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args,
	                optional_ptr<vector<LogicalType>> order_types = nullptr,
	                optional_ptr<vector<LogicalType>> arg_order_types = nullptr) {
		return ResolveFunction(make_shared_ptr<WindowFunction>(function), children, keyword_args, order_types,
		                       arg_order_types);
	}

	pair<BoundWindowFunction, unique_ptr<FunctionData>> ResolveFunction(const WindowFunction &function,
	                                                                    vector<unique_ptr<Expression>> &children) {
		return ResolveFunction(make_shared_ptr<WindowFunction>(function), children);
	}

private:
	//! Resolves the argument / return types of the function and casts the children to them - everything that happens
	//! before the bind callback is invoked
	ResolvedScalarFunctionTypes
	ResolveScalarFunctionTypes(shared_ptr<const ScalarFunction> function, vector<unique_ptr<Expression>> &children,
	                           vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args);
	//! Cast a set of expressions to the arguments of this function
	void CastToFunctionArguments(BoundSimpleFunction &function, vector<unique_ptr<Expression>> &children);

	void ResolveTemplateTypes(BoundSimpleFunction &bound_function, const vector<unique_ptr<Expression>> &children);
	void CheckTemplateTypesResolved(const BoundSimpleFunction &bound_function);

	//! Select the best matching overload for the given full (maybe-named) argument list. Names positional arguments by
	//! their alias if that is the only way the call matches an overload that captures argument aliases
	template <class T>
	FunctionBinding SelectFunctionFromArguments(const Identifier &name, const FunctionSet<T> &functions,
	                                            vector<pair<Identifier, unique_ptr<Expression>>> &arguments,
	                                            ErrorData &error);

	static pair<vector<LogicalType>, vector<pair<Identifier, LogicalType>>>
	GetArgumentsFromExpressions(const vector<unique_ptr<Expression>> &regular_arguments,
	                            const vector<pair<Identifier, unique_ptr<Expression>>> &keyword_arguments);
};

} // namespace duckdb
