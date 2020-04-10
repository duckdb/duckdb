//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/table_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"
#include "duckdb/main/table_description.hpp"

namespace duckdb {

class TableRelation : public Relation {
public:
	TableRelation(ClientContext &context, unique_ptr<TableDescription> description);

	unique_ptr<TableDescription> description;

public:
	unique_ptr<QueryNode> GetQueryNode() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;

	unique_ptr<TableRef> GetTableRef() override;

	void Update(string update, string condition = string()) override;
	void Delete(string condition = string()) override;
};

} // namespace duckdb
