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

class BoundMacroExpression;
class MacroFunctionCatalogEntry;

class MacroFunction {
public:
	MacroFunction(unique_ptr<ParsedExpression> expression);
	static unique_ptr<BoundMacroExpression> BindMacroFunction(ExpressionBinder &binder,
	                                                          MacroFunctionCatalogEntry &function,
                                                              vector<unique_ptr<ParsedExpression>> parsed_children,
                                                              vector<unique_ptr<Expression>> bound_children, string &error);
	//! The macro expression
	unique_ptr<ParsedExpression> expression;
	//! The macro arguments
	vector<unique_ptr<ParsedExpression>> arguments;
};

} // namespace duckdb
