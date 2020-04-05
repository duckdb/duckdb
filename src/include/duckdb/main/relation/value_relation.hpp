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
	ValueRelation(ClientContext &context, vector<vector<Value>> values, vector<string> names, string alias = "values");

	vector<vector<Value>> values;
	vector<ColumnDefinition> columns;
	string alias;
public:
	unique_ptr<QueryNode> GetQueryNode() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;

	unique_ptr<TableRef> GetTableRef() override;
};

} // namespace duckdb
