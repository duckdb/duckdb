//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table_macro_function.hpp
//
//
//===----------------------------------------------------------------------===//

//! The SelectStatement of the view
#include "duckdb/function/table_macro_function.hpp"
#include  "duckdb/function/macro_function.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {


TableMacroFunction::TableMacroFunction(unique_ptr<QueryNode> query_node)
    : MacroFunction(MacroType::TABLE_MACRO) , query_node(move(query_node)){ }

TableMacroFunction::TableMacroFunction(void) : MacroFunction(MacroType::TABLE_MACRO) { }

TableMacroFunction::~TableMacroFunction() {}


  unique_ptr<MacroFunction> TableMacroFunction::Copy() {
		auto result= make_unique<TableMacroFunction>();
	    result->query_node=query_node->Copy();
		this->CopyProperties(*result);
	    return move( result);
	}

    } // namespace duckdb
