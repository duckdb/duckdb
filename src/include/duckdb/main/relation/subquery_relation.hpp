//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/subquery_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"

namespace duckdb {

class SubqueryRelation : public Relation {
public:
	SubqueryRelation(shared_ptr<Relation> child, string alias);

	shared_ptr<Relation> child;
	string alias;

public:
	unique_ptr<QueryNode> GetQueryNode() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;

public:
	bool InheritsColumnBindings() override {
		return child->InheritsColumnBindings();
	}
	Relation *ChildRelation() override {
		return child->ChildRelation();
	}
};

} // namespace duckdb
