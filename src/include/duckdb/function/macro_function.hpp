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
//#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"

namespace duckdb {


enum class MacroType : uint8_t {
	VOID_MACRO  = 0,
	TABLE_MACRO = 1,
	SCALAR_MACRO = 2
};


class BaseMacroCatalogEntry;

class MacroFunction {
public:
	//explicit MacroFunction(unique_ptr<ParsedExpression> expression);
	MacroFunction(MacroType type);

	//MacroFunction(void);
	//! Check whether the supplied arguments are valid
	static string ValidateArguments(BaseMacroCatalogEntry &macro_func, FunctionExpression &function_expr,
	                                vector<unique_ptr<ParsedExpression>> &positionals,
	                                unordered_map<string, unique_ptr<ParsedExpression>> &defaults);

	// The type
 	MacroType  type;
	//! The positional parameters
	vector<unique_ptr<ParsedExpression>> parameters;
	//! The default parameters and their associated values
	unordered_map<string, unique_ptr<ParsedExpression>> default_parameters;

public:
	virtual ~MacroFunction() {};

	void CopyProperties( MacroFunction &other);

	virtual unique_ptr<MacroFunction> Copy()=0;


};

} // namespace duckdb
