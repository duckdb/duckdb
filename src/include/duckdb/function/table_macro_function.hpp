//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table_macro_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
//! The SelectStatement of the view
#include "duckdb/function/macro_function.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

class TableMacroFunction : public MacroFunction {
public:
	TableMacroFunction(unique_ptr<QueryNode> query_node);
	TableMacroFunction(void);

	//! The main query node
	unique_ptr<QueryNode> query_node;

public:
	unique_ptr<MacroFunction> Copy() override;
};

} // namespace duckdb
