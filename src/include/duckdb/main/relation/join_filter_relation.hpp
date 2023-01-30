//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/join_filter_elation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"

namespace duckdb {

class JoinFilterRelation : public Relation {
public:
	DUCKDB_API JoinFilterRelation(shared_ptr<Relation> left, shared_ptr<Relation> left_expr, shared_ptr<Relation> right,
	                        JoinType type);

	shared_ptr<Relation> left;
	shared_ptr<Relation> right;
	shared_ptr<Relation> left_expr;
	JoinType join_type;
	vector<ColumnDefinition> columns;

public:
	unique_ptr<QueryNode> GetQueryNode() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;

	unique_ptr<TableRef> GetTableRef() override;
};

} // namespace duckdb
