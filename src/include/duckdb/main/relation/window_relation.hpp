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

namespace duckdb {

class WindowRelation : public Relation {
public:
	WindowRelation(shared_ptr<Relation> table, unique_ptr<ParsedExpression> child_, vector<ColumnRefExpression> partitions_);

	string schema_name;
	string function_name;

	shared_ptr<Relation> from_table;

	vector<ColumnDefinition> columns;

	unique_ptr<ParsedExpression> child;
	vector<ColumnRefExpression> partitions;

	vector<std::shared_ptr<Relation>> orders;
	unique_ptr<ParsedExpression> filter_expr;

	unique_ptr<Relation> start_expr;
	unique_ptr<Relation> end_expr;
	//! Offset and default expressions for WINDOW_LEAD and WINDOW_LAG functions
	unique_ptr<Relation> offset_expr;
	unique_ptr<Relation> default_expr;

public:
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<TableRef> GetTableRef() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;
};

} // namespace duckdb
