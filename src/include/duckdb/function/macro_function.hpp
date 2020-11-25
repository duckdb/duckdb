//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/macro_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

class MacroCatalogEntry;

class MacroFunction {
public:
	MacroFunction(unique_ptr<ParsedExpression> expression);

	//! Check whether the supplied arguments are valid
	static string ValidateArguments(MacroCatalogEntry &macro_func, FunctionExpression &function_expr);
	//! The macro expression
	unique_ptr<ParsedExpression> expression;
	//! The macro parameters
	vector<unique_ptr<ParsedExpression>> parameters;
};

} // namespace duckdb
