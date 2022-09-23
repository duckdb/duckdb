//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/read_csv_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"

namespace duckdb {

class ReadCSVRelation : public Relation {
public:
	ReadCSVRelation(const std::shared_ptr<ClientContext> &context, string csv_file, vector<ColumnDefinition> columns,
	                bool auto_detect = false, string alias = string());

	string csv_file;
	bool auto_detect;
	string alias;
	vector<ColumnDefinition> columns;

public:
	unique_ptr<QueryNode> GetQueryNode() override;
	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;
	unique_ptr<TableRef> GetTableRef() override;
};

} // namespace duckdb
