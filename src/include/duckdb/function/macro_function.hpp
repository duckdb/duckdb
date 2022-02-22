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


enum class MacroType : uint8_t {
	VOID_MACRO  = 0,
	TABLE_MACRO = 1,
	SCALAR_MACRO = 2
};


class MacroCatalogEntry;

class MacroFunction {
public:
	//explicit MacroFunction(unique_ptr<ParsedExpression> expression);
	MacroFunction(MacroType type);

	//MacroFunction(void);
	//! Check whether the supplied arguments are valid
	static string ValidateArguments(MacroCatalogEntry &macro_func, FunctionExpression &function_expr,
	                                vector<unique_ptr<ParsedExpression>> &positionals,
	                                unordered_map<string, unique_ptr<ParsedExpression>> &defaults);

	virtual ~MacroFunction() {};

	// The type
 	MacroType  type;
	//! The positional parameters
	vector<unique_ptr<ParsedExpression>> parameters;
	//! The default parameters and their associated values
	unordered_map<string, unique_ptr<ParsedExpression>> default_parameters;

	unique_ptr<QueryNode> query_node;


public:
	void CopyProperties( MacroFunction &other);

	virtual unique_ptr<MacroFunction> Copy()=0;


};

} // namespace duckdb
