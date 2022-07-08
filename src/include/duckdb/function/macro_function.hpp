//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/macro_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/query_node.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

enum class MacroType : uint8_t { VOID_MACRO = 0, TABLE_MACRO = 1, SCALAR_MACRO = 2 };

class MacroFunction {
public:
	MacroFunction(MacroType type);

	//! The type
	MacroType type;
	//! The positional parameters
	vector<unique_ptr<ParsedExpression>> parameters;
	//! The default parameters and their associated values
	unordered_map<string, unique_ptr<ParsedExpression>> default_parameters;

public:
	virtual ~MacroFunction() {
	}

	void CopyProperties(MacroFunction &other);

	virtual unique_ptr<MacroFunction> Copy() = 0;

	static string ValidateArguments(MacroFunction &macro_function, const string &name,
	                                FunctionExpression &function_expr,
	                                vector<unique_ptr<ParsedExpression>> &positionals,
	                                unordered_map<string, unique_ptr<ParsedExpression>> &defaults);

	virtual string ToSQL(const string &schema, const string &name);
};

} // namespace duckdb
