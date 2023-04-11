//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table_macro_function.hpp
//
//
//===----------------------------------------------------------------------===//
//! The SelectStatement of the view
#include "duckdb/function/table_macro_function.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

TableMacroFunction::TableMacroFunction(unique_ptr<QueryNode> query_node)
    : MacroFunction(MacroType::TABLE_MACRO), query_node(std::move(query_node)) {
}

TableMacroFunction::TableMacroFunction(void) : MacroFunction(MacroType::TABLE_MACRO) {
}

unique_ptr<MacroFunction> TableMacroFunction::Copy() const {
	auto result = make_uniq<TableMacroFunction>();
	result->query_node = query_node->Copy();
	this->CopyProperties(*result);
	return std::move(result);
}

string TableMacroFunction::ToSQL(const string &schema, const string &name) const {
	return MacroFunction::ToSQL(schema, name) + StringUtil::Format("TABLE (%s);", query_node->ToString());
}

} // namespace duckdb
