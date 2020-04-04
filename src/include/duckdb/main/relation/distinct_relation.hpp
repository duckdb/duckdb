//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/distinct_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"

namespace duckdb {

class DistinctRelation : public Relation {
public:
	DistinctRelation(shared_ptr<Relation> child);

	shared_ptr<Relation> child;
public:
	BoundStatement Bind(Binder &binder) override;
	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
};

} // namespace duckdb
