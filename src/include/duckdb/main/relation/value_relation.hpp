//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/value_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"

namespace duckdb {

class ValueRelation : public Relation {
public:
	ValueRelation(ClientContext &context, vector<vector<Value>> values, vector<string> names);

	vector<vector<Value>> values;
	vector<ColumnDefinition> columns;
public:
	unique_ptr<QueryNode> GetQueryNode() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
};

} // namespace duckdb
