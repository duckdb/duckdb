//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/builtin_function_lookup.hpp
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
class BoundFunctionExpression;
class ClientContext;
class Expression;
class ScalarFunction;

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

//! Look up a built-in scalar function as GetBuiltinScalarFunction does, and bind it to the given children
unique_ptr<BoundFunctionExpression> BindBuiltinScalarFunction(ClientContext &context, const Identifier &name,
                                                              vector<unique_ptr<Expression>> children);

} // namespace duckdb
