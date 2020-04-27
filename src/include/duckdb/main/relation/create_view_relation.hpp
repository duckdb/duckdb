//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/create_view_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"

namespace duckdb {

class CreateViewRelation : public Relation {
public:
	CreateViewRelation(shared_ptr<Relation> child, string view_name, bool replace);

	shared_ptr<Relation> child;
	string view_name;
	bool replace;
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
