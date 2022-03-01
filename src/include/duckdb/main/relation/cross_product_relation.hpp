//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/cross_product_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"

namespace duckdb {

class CrossProductRelation : public Relation {
public:
	DUCKDB_API CrossProductRelation(shared_ptr<Relation> left, shared_ptr<Relation> right);

	shared_ptr<Relation> left;
	shared_ptr<Relation> right;
	vector<ColumnDefinition> columns;

public:
	unique_ptr<QueryNode> GetQueryNode() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;

	unique_ptr<TableRef> GetTableRef() override;
};

} // namespace duckdb
