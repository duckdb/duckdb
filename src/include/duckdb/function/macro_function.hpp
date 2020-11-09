//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/macro_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

class MacroFunctionCatalogEntry;

class MacroFunction {
public:
	MacroFunction(unique_ptr<ParsedExpression> expression);
	static unique_ptr<Expression> BindMacroFunction(ExpressionBinder &binder, MacroFunctionCatalogEntry &function,
	                                                vector<unique_ptr<ParsedExpression>> children);
	//! The macro expression
	unique_ptr<ParsedExpression> expression;
	//! The macro arguments
	vector<unique_ptr<ParsedExpression>> arguments;
};

} // namespace duckdb
