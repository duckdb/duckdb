//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/macro_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"

namespace duckdb {

class BoundMacroExpression;
class MacroFunctionCatalogEntry;

class MacroFunction {
public:
	MacroFunction(unique_ptr<ParsedExpression> expression);
	static unique_ptr<BoundMacroExpression> BindMacroFunction(ClientContext &context,
	                                                          MacroFunctionCatalogEntry &function,
	                                                          vector<unique_ptr<Expression>> children, string &error);
	//! The macro expression
	unique_ptr<ParsedExpression> expression;
	//! The macro arguments
	vector<unique_ptr<ParsedExpression>> arguments;
};

} // namespace duckdb
