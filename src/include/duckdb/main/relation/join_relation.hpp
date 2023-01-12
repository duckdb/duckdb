//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/join_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"

namespace duckdb {

class JoinRelation : public Relation {
public:
	DUCKDB_API JoinRelation(shared_ptr<Relation> left, shared_ptr<Relation> right,
	                        unique_ptr<ParsedExpression> condition, JoinType type);
	DUCKDB_API JoinRelation(shared_ptr<Relation> left, shared_ptr<Relation> right, vector<string> using_columns,
	                        JoinType type);
	// for anti and semi joins
	DUCKDB_API JoinRelation(shared_ptr<Relation> left, shared_ptr<Relation> left_expr, shared_ptr<Relation> right, JoinType type);

	shared_ptr<Relation> left;
	shared_ptr<Relation> right;
	// for anti and smei joins
	// Select * from left where left_expr NOT IN (right_expr). right becomes treated as a right_expr
	shared_ptr<Relation> left_expr;
	unique_ptr<ParsedExpression> condition;
	vector<string> using_columns;
	JoinType join_type;
	vector<ColumnDefinition> columns;

public:
	unique_ptr<QueryNode> GetQueryNode() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;

	unique_ptr<TableRef> GetTableRef() override;
};

} // namespace duckdb
