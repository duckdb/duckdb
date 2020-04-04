//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/setop_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"

namespace duckdb {

class SetOpRelation : public Relation {
public:
	SetOpRelation(shared_ptr<Relation> left, shared_ptr<Relation> right, LogicalOperatorType setop_type);

	shared_ptr<Relation> left;
	shared_ptr<Relation> right;
	LogicalOperatorType setop_type;
public:
	BoundStatement Bind(Binder &binder) override;
	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
};

} // namespace duckdb
