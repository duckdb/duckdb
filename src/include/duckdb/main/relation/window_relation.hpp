//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/window_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/parser/expression/window_expression.hpp"

namespace duckdb {

class WindowRelation : public Relation {
public:
	WindowRelation(shared_ptr<Relation> rel, std::string window_function,
	               vector<unique_ptr<ParsedExpression>> children_, std::string window_alias_name,
	               vector<unique_ptr<ParsedExpression>> partitions_, vector<unique_ptr<OrderByNode>> orders_,
	               unique_ptr<ParsedExpression> filter_expr_, WindowBoundary start_, WindowBoundary end_,
	               vector<unique_ptr<ParsedExpression>> start_end_offset_default);

	string alias;
	string window_function;

	shared_ptr<Relation> from_table;

	vector<ColumnDefinition> columns;

	vector<unique_ptr<ParsedExpression>> children;
	vector<unique_ptr<ParsedExpression>> partitions;

	vector<unique_ptr<OrderByNode>> orders;
	unique_ptr<ParsedExpression> filter_expr;

	//! The window boundaries
	WindowBoundary start = WindowBoundary::INVALID;
	WindowBoundary end = WindowBoundary::INVALID;

	unique_ptr<ParsedExpression> start_expr;
	unique_ptr<ParsedExpression> end_expr;
	//! Offset and default expressions for WINDOW_LEAD and WINDOW_LAG functions
	unique_ptr<ParsedExpression> offset_expr;
	unique_ptr<ParsedExpression> default_expr;

	vector<unique_ptr<ParsedExpression>> table_ref_children;

public:
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<TableRef> GetTableRef() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;
};

} // namespace duckdb
