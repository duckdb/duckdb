//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/macro_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
//! The SelectStatement of the view
#include "duckdb/parser/query_node.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

class MacroCatalogEntry;

class MacroFunction {
public:
	explicit MacroFunction(unique_ptr<ParsedExpression> expression);
	MacroFunction(void);

	bool isQuery(void);

	//! Check whether the supplied arguments are valid
	static string ValidateArguments(MacroCatalogEntry &macro_func, FunctionExpression &function_expr,
	                                vector<unique_ptr<ParsedExpression>> &positionals,
	                                unordered_map<string, unique_ptr<ParsedExpression>> &defaults);
	//! The macro expression
	unique_ptr<ParsedExpression> expression;
	//! The positional parameters
	vector<unique_ptr<ParsedExpression>> parameters;
	//! The default parameters and their associated values
	unordered_map<string, unique_ptr<ParsedExpression>> default_parameters;

	// if true then we have a query_node and not a regular expression
	// bool isQuery;
	//! The main query node
	unique_ptr<QueryNode> query_node;

public:
	unique_ptr<MacroFunction> Copy();
};

} // namespace duckdb
