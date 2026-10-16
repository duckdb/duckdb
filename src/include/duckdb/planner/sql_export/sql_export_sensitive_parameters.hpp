//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/sql_export/sql_export_sensitive_parameters.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {
class ClientContext;
class Expression;

//! Resolves rendered invocation arguments against sensitive parameter declarations
struct SQLExportSensitiveParameters {
	//! Whether the retained value is the parameter's declared default - registration metadata, not a secret
	static bool MatchesDeclaredDefault(ClientContext &context, const FunctionParameter &parameter, const Value &value);
	//! The declared parameter that receives the positional argument at the given index
	static optional_ptr<const FunctionParameter> PositionalParameter(const FunctionSignature &signature, idx_t index);
	//! The name to report when the positional argument at the index must not render, empty when rendering is allowed
	static Identifier RefusePositional(optional_ptr<ClientContext> context, const FunctionSignature &signature,
	                                   idx_t index, optional_ptr<const Value> value);
	//! The name to report when the named argument must not render, empty when rendering is allowed
	static Identifier RefuseNamed(optional_ptr<ClientContext> context, const FunctionSignature &signature,
	                              const Identifier &name, optional_ptr<const Value> value);
	//! The name to report when the bound argument at the index of a [positional | named tail] call layout must not
	//! render, empty when rendering is allowed
	static Identifier RefuseBoundArgument(optional_ptr<ClientContext> context, const FunctionSignature &signature,
	                                      idx_t positional_count, const vector<Identifier> &named_arguments,
	                                      idx_t index, const Expression &argument);
};
} // namespace duckdb
