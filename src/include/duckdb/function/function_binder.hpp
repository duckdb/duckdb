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
struct BoundPragmaInfo;

//! One ORDER BY key of an exported ordered aggregate state: the buffered struct column it sorts on and the modifiers.
struct SortedAggregateStateOrder {
	idx_t column;
	OrderType order_type;
	OrderByNullType null_order;
};

//! A scalar function with its argument and return types resolved, before the bind callback is invoked
struct ResolvedScalarFunctionTypes {
	BoundScalarFunction bound_function;
	//! The collation pushed into the arguments, to be propagated to the return type after binding
	string pushed_collation;
};

//! A table function bound to the constants it is called with, before its bind callback is invoked
struct ResolvedTableFunction {
	explicit ResolvedTableFunction(BoundTableFunction function_p, vector<Value> parameters_p = {},
	                               named_argument_map_t named_parameters_p = {})
	    : function(std::move(function_p)), parameters(std::move(parameters_p)),
	      named_parameters(std::move(named_parameters_p)) {
	}

	BoundTableFunction function;
	vector<Value> parameters;
	named_argument_map_t named_parameters;
};

//! An overload that matches a call, and how the arguments of the call are passed to it
struct FunctionBinding {
	//! The index of the overload in the function set
	idx_t GetIndex() const {
		return function_index;
	}

	//! The index of the overload in the function set
	idx_t function_index = 0;
	//! The cost of casting the arguments to the parameters of the overload
	idx_t cost = 0;
	//! The parameter of the overload that receives each argument - the positional arguments, followed by the named
	//! ones. Arguments received by "*args" or "**kwargs" map to that parameter
	vector<idx_t> argument_parameters;
	//! The type of every parameter of the overload, with the templates the arguments and defaults determine
	//! substituted
	vector<LogicalType> parameter_types;
	//! The return type of the overload, with the templates substituted
	LogicalType return_type;
};

//! Why an overload does not match a call
enum class FunctionMismatchReason : uint8_t {
	//! A parameter without a default is not filled by the call
	MISSING_PARAMETER,
	//! The call passes more arguments than the overload takes
	TOO_MANY_ARGUMENTS,
	//! A named argument names no parameter, and the overload has no "**kwargs" to receive it
	UNKNOWN_NAMED_ARGUMENT,
	//! A named argument names a parameter that a positional argument already fills
	ALREADY_PASSED_BY_POSITION,
	//! An argument cannot be implicitly cast to the type of the parameter that receives it
	INCOMPATIBLE_ARGUMENT,
	//! The arguments determine conflicting types for a template of the overload
	TEMPLATE_DEDUCTION,
	//! Neither the arguments nor the defaults determine a template of the types the call uses
	TEMPLATE_UNRESOLVED
};

//! An overload that does not match a call, and why
struct FailedFunctionCandidate {
	//! The index of the overload in the function set
	idx_t function_index = 0;
	FunctionMismatchReason reason = FunctionMismatchReason::TOO_MANY_ARGUMENTS;
	//! The argument the reason refers to - the positional arguments, followed by the named ones. Invalid if the reason
	//! refers to no single one
	optional_idx position;
	//! The parameter that is missing - only set for MISSING_PARAMETER
	optional_idx parameter;
	//! How far the call is from matching the overload, lower is closer. Any number of arguments that cannot be cast
	//! is closer than a call of the wrong shape - missing parameters, extra arguments or unknown names
	idx_t distance = 0;
	//! The template that cannot be deduced - only set for TEMPLATE_DEDUCTION and TEMPLATE_UNRESOLVED
	string template_name;
	//! The types the template was inferred to be, in the order they were inferred, followed by the type that
	//! conflicts with them - only set for TEMPLATE_DEDUCTION
	vector<LogicalType> inferred_types;
};

enum class FunctionSelectionResult : uint8_t { SUCCESS, AMBIGUOUS, NO_MATCH };

//! The outcome of selecting the overload of a function set that a call binds to
class FunctionSelection {
public:
	FunctionSelectionResult GetResult() const {
		return result;
	}
	bool Succeeded() const {
		return result == FunctionSelectionResult::SUCCESS;
	}
	//! The overload the call binds to - only if the selection succeeded
	const FunctionBinding &GetBinding() const {
		D_ASSERT(Succeeded());
		return candidates[0];
	}
	//! The overloads that match the call, cheapest first. At most the limit the selection was made with
	const vector<FunctionBinding> &GetCandidates() const {
		return candidates;
	}
	//! The overloads that do not match the call, closest first. At most the limit the selection was made with - the
	//! closest ones are kept
	const vector<FailedFunctionCandidate> &GetFailedCandidates() const {
		return failed_candidates;
	}
	//! The number of overloads that match the call, including those beyond the limit
	idx_t GetCandidateCount() const {
		return candidate_count;
	}
	//! The number of overloads that do not match the call, including those beyond the limit
	idx_t GetFailedCandidateCount() const {
		return failed_candidate_count;
	}
	//! Why the selection did not succeed
	const ErrorData &GetError() const {
		return error;
	}
	//! The argument the error is about, if it is about a single one - the positional arguments, followed by the named
	//! ones. A caller that has the argument expressions points the error at it with AddErrorLocation
	optional_idx GetErrorArgument() const {
		return error_argument;
	}
	void AddErrorLocation(QueryLocation location) {
		error.AddQueryLocation(location);
	}

	//! Shorthands for Succeeded() and GetBinding().GetIndex(), so that a selection reads like the index it selects
	bool IsValid() const {
		return Succeeded();
	}
	idx_t GetIndex() const {
		return GetBinding().GetIndex();
	}

private:
	friend class FunctionBinder;

	FunctionSelectionResult result = FunctionSelectionResult::NO_MATCH;
	vector<FunctionBinding> candidates;
	vector<FailedFunctionCandidate> failed_candidates;
	idx_t candidate_count = 0;
	idx_t failed_candidate_count = 0;
	ErrorData error;
	optional_idx error_argument;
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
	//! to. Selection needs no binder - without a ClientContext only the built-in implicit casts are considered. The
	//! limit bounds how many matching and how many failed overloads the selection keeps, it does not change the result.
	//! With explicit casts, an argument that cannot be implicitly cast to its parameter is accepted too
	DUCKDB_API static FunctionSelection
	SelectFunction(optional_ptr<ClientContext> context, const BaseFunctionSet &functions,
	               const vector<LogicalType> &arguments,
	               const vector<pair<Identifier, LogicalType>> &named_arguments = {},
	               optional_idx limit = optional_idx(), bool explicit_casts = false);
	//! Selects by the return types of the bound arguments
	DUCKDB_API static FunctionSelection
	SelectFunction(optional_ptr<ClientContext> context, const BaseFunctionSet &functions,
	               const vector<unique_ptr<Expression>> &arguments,
	               const vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments,
	               optional_idx limit = optional_idx(), bool explicit_casts = false);

	//! Bind a table function: select the overload, check the named arguments against its signature, and fold the
	//! arguments to the constants it is called with, cast to the types it declares. Returns nullptr and sets the error
	//! if no overload matches
	DUCKDB_API unique_ptr<ResolvedTableFunction>
	BindTableFunction(const TableFunctionSet &functions, vector<unique_ptr<Expression>> &positional_arguments,
	                  vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments, ErrorData &error);
	//! Bind a pragma function, as BindTableFunction binds a table function
	DUCKDB_API unique_ptr<BoundPragmaInfo>
	BindPragmaFunction(const PragmaFunctionSet &functions, vector<unique_ptr<Expression>> &positional_arguments,
	                   vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments, ErrorData &error);

	//! Casts a constant to the type of the parameter it fills, as an argument of any function is cast - a parameter
	//! whose type is not concrete, such as ANY, takes it as it is
	DUCKDB_API static Value CastToParameterType(ClientContext &context, Value value, const LogicalType &parameter_type);

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
	DUCKDB_API unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(const AggregateFunctionCatalogEntry &function, vector<unique_ptr<Expression>> children,
	                      ErrorData &error, unique_ptr<Expression> filter = nullptr,
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

	DUCKDB_API unique_ptr<BoundWindowExpression>
	BindWindowFunction(const WindowFunctionCatalogEntry &function,
	                   vector<pair<Identifier, unique_ptr<Expression>>> arguments, ErrorData &error,
	                   vector<LogicalType> &order_types, vector<LogicalType> &arg_order_types);

	DUCKDB_API unique_ptr<Expression> BindScalarWindowFunction(BoundWindowExpression &wexpr,
	                                                           const vector<LogicalType> &input_types);

	//! Turn a function into a bound function with how the call binds to it, from the selection of the overload - the
	//! arguments are passed as they were to the selection, positional ones first. The function is kept as the
	//! definition of the resulting bound function - see BoundScalarFunction::GetDefinition()
	DUCKDB_API pair<BoundScalarFunction, unique_ptr<FunctionData>>
	ResolveFunction(shared_ptr<const ScalarFunction> function, const FunctionBinding &binding,
	                vector<unique_ptr<Expression>> &children,
	                vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args);
	DUCKDB_API pair<BoundAggregateFunction, unique_ptr<FunctionData>>
	ResolveFunction(shared_ptr<const AggregateFunction> function, const FunctionBinding &binding,
	                vector<unique_ptr<Expression>> &children,
	                vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args);
	DUCKDB_API pair<BoundWindowFunction, unique_ptr<FunctionData>>
	ResolveFunction(shared_ptr<const WindowFunction> function, const FunctionBinding &binding,
	                vector<unique_ptr<Expression>> &children,
	                vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args,
	                optional_ptr<vector<LogicalType>> order_types = nullptr,
	                optional_ptr<vector<LogicalType>> arg_order_types = nullptr);

private:
	//! Resolves the argument / return types of the function and casts the children to them - everything that happens
	//! before the bind callback is invoked
	ResolvedScalarFunctionTypes
	ResolveScalarFunctionTypes(shared_ptr<const ScalarFunction> function, const FunctionBinding &binding,
	                           vector<unique_ptr<Expression>> &children,
	                           vector<pair<Identifier, unique_ptr<Expression>>> &keyword_args);

	unique_ptr<Expression> BindScalarFunction(shared_ptr<const ScalarFunction> function, const FunctionBinding &binding,
	                                          vector<unique_ptr<Expression>> children,
	                                          vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
	                                          bool is_operator, optional_ptr<Binder> binder);
	unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(shared_ptr<const AggregateFunction> function, FunctionBinding binding,
	                      vector<unique_ptr<Expression>> children,
	                      vector<pair<Identifier, unique_ptr<Expression>>> keyword_args, unique_ptr<Expression> filter,
	                      AggregateType aggr_type);
	unique_ptr<BoundWindowExpression> BindWindowFunction(shared_ptr<const WindowFunction> function,
	                                                     const FunctionBinding &binding,
	                                                     vector<unique_ptr<Expression>> children,
	                                                     vector<pair<Identifier, unique_ptr<Expression>>> keyword_args,
	                                                     vector<LogicalType> &order_types,
	                                                     vector<LogicalType> &arg_order_types);
	//! Cast the arguments of a call to the types of the parameters they fill
	void CastToFunctionArguments(const Identifier &function_name, vector<LogicalType> &argument_types,
	                             vector<unique_ptr<Expression>> &children);

	//! Select the best matching overload for the given full (maybe-named) argument list. Names positional arguments by
	//! their alias if that is the only way the call matches an overload that captures argument aliases
	FunctionSelection SelectFunctionFromArguments(const BaseFunctionSet &functions,
	                                              vector<pair<Identifier, unique_ptr<Expression>>> &arguments);

	//! Whether an overload matches a call - with how the call binds to it if it does, and why not if it does not. With
	//! explicit casts, an argument that cannot be implicitly cast to its parameter is accepted too
	static bool MatchOverload(optional_ptr<ClientContext> context, const SimpleFunction &function,
	                          const vector<LogicalType> &arguments,
	                          const vector<pair<Identifier, LogicalType>> &named_arguments, bool explicit_casts,
	                          FunctionBinding &binding, FailedFunctionCandidate &failure);
	//! How a call binds to a function it is known to call - the arguments are cast explicitly. Throws if the call does
	//! not fit the signature of the function
	template <class T>
	FunctionBinding MatchFunction(const shared_ptr<const T> &function, const vector<unique_ptr<Expression>> &arguments,
	                              const vector<pair<Identifier, unique_ptr<Expression>>> &named_arguments);

	static pair<vector<LogicalType>, vector<pair<Identifier, LogicalType>>>
	GetArgumentsFromExpressions(const vector<unique_ptr<Expression>> &regular_arguments,
	                            const vector<pair<Identifier, unique_ptr<Expression>>> &keyword_arguments);
};

} // namespace duckdb
