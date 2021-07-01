//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/query_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {
class SelectStatement;

class QueryRelation : public Relation {
public:
	QueryRelation(ClientContext &context, string query, string alias = "query_relation");

	string query;
	string alias;
	vector<ColumnDefinition> columns;

public:
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<TableRef> GetTableRef() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;

private:
	unique_ptr<SelectStatement> GetSelectStatement();
};

} // namespace duckdb
