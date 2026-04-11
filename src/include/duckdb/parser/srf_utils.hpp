//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/srf_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/to_string.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

// Wrap a table function call as: (SELECT list(__c) FROM (SELECT * FROM func()) __t(__c))
// Returns a scalar subquery expression producing a LIST from the table function's results.
inline unique_ptr<ParsedExpression> WrapTableFuncAsList(unique_ptr<ParsedExpression> func_expr, idx_t idx = 0) {
	// Inner: SELECT * FROM func()
	auto inner_select = make_uniq<SelectNode>();
	inner_select->select_list.push_back(make_uniq<StarExpression>());
	auto table_ref = make_uniq<TableFunctionRef>();
	table_ref->function = std::move(func_expr);
	inner_select->from_table = std::move(table_ref);
	auto inner_stmt = make_uniq<SelectStatement>();
	inner_stmt->node = std::move(inner_select);

	// Wrap: (...) __srf_N(__c)
	auto subquery_ref = make_uniq<SubqueryRef>(std::move(inner_stmt), "__srf_" + to_string(idx));
	subquery_ref->column_name_alias.push_back("__c");

	// Outer: SELECT list(__c) FROM __srf_N
	auto outer_select = make_uniq<SelectNode>();
	auto list_func = make_uniq<FunctionExpression>("list", vector<unique_ptr<ParsedExpression>> {});
	list_func->children.push_back(make_uniq<ColumnRefExpression>("__c"));
	outer_select->select_list.push_back(std::move(list_func));
	outer_select->from_table = std::move(subquery_ref);
	auto outer_stmt = make_uniq<SelectStatement>();
	outer_stmt->node = std::move(outer_select);

	// Scalar subquery returning LIST
	auto result = make_uniq<SubqueryExpression>();
	result->subquery = std::move(outer_stmt);
	result->subquery_type = SubqueryType::SCALAR;
	return result;
}

} // namespace duckdb
