//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/builtin_function_lookup.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class AggregateFunction;
class ClientContext;
class Expression;
class ScalarFunction;
class WindowFunction;

//! Look up a built-in scalar function in the system catalog and select the overload matching the given argument
//! types. Rewrites that introduce a registered built-in resolve it this way so that the bound function keeps the
//! catalog and schema name of its definition, like any function bound from SQL.
shared_ptr<const ScalarFunction> GetBuiltinScalarFunction(ClientContext &context, const Identifier &name,
                                                          const vector<LogicalType> &arguments);
//! Aggregate counterpart of GetBuiltinScalarFunction
shared_ptr<const AggregateFunction> GetBuiltinAggregateFunction(ClientContext &context, const Identifier &name,
                                                                const vector<LogicalType> &arguments);
//! Like GetBuiltinAggregateFunction, but returns nullptr instead of throwing when no overload matches
shared_ptr<const AggregateFunction> TryGetBuiltinAggregateFunction(ClientContext &context, const Identifier &name,
                                                                   const vector<LogicalType> &arguments);

//! Window counterpart of GetBuiltinScalarFunction
shared_ptr<const WindowFunction> GetBuiltinWindowFunction(ClientContext &context, const Identifier &name,
                                                          const vector<LogicalType> &arguments);

//! Look up a built-in scalar function as GetBuiltinScalarFunction does, and bind it to the given children.
//! The call is bound like one written in SQL, so the result is whatever the bind produces and not necessarily a
//! BoundFunctionExpression: the bind can fold the call into a constant (e.g. a call with a NULL constant argument),
//! and a function's bind_expression callback can replace it with a different expression altogether
unique_ptr<Expression> BindBuiltinScalarFunction(ClientContext &context, const Identifier &name,
                                                 vector<unique_ptr<Expression>> children);

} // namespace duckdb
