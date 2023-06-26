//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/join_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"
#include "duckdb/common/enums/joinref_type.hpp"

namespace duckdb {

class JoinRelation : public Relation {
public:
	DUCKDB_API JoinRelation(shared_ptr<Relation> left, shared_ptr<Relation> right,
	                        unique_ptr<ParsedExpression> condition, JoinType type,
	                        JoinRefType join_ref_type = JoinRefType::REGULAR);
	DUCKDB_API JoinRelation(shared_ptr<Relation> left, shared_ptr<Relation> right, vector<string> using_columns,
	                        JoinType type, JoinRefType join_ref_type = JoinRefType::REGULAR);

	shared_ptr<Relation> left;
	shared_ptr<Relation> right;
	unique_ptr<ParsedExpression> condition;
	vector<string> using_columns;
	JoinType join_type;
	JoinRefType join_ref_type;
	vector<ColumnDefinition> columns;

public:
	unique_ptr<QueryNode> GetQueryNode() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;

	unique_ptr<TableRef> GetTableRef() override;
};

} // namespace duckdb
