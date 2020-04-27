//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/explain_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"

namespace duckdb {

class ExplainRelation : public Relation {
public:
	ExplainRelation(shared_ptr<Relation> child);

	shared_ptr<Relation> child;
	vector<ColumnDefinition> columns;

public:
	unique_ptr<QueryNode> GetQueryNode() override;
	BoundStatement Bind(Binder &binder) override;
	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	bool IsReadOnly() override {
		return false;
	}
};

} // namespace duckdb
