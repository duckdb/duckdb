//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/limit_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

class LimitRelation : public Relation {
public:
	LimitRelation(shared_ptr<Relation> child, int64_t limit, int64_t offset);

	int64_t limit;
	int64_t offset;
	shared_ptr<Relation> child;
public:
	BoundStatement Bind(Binder &binder) override;
	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
};

} // namespace duckdb
