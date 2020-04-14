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
	JoinRelation(shared_ptr<Relation> left, shared_ptr<Relation> right, unique_ptr<ParsedExpression> condition,
	             JoinType type);
	JoinRelation(shared_ptr<Relation> left, shared_ptr<Relation> right, vector<string> using_columns, JoinType type);

	shared_ptr<Relation> left;
	shared_ptr<Relation> right;
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
