//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/delim_get_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"

namespace duckdb {

class DelimGetRelation : public Relation {
public:
	DUCKDB_API DelimGetRelation(const shared_ptr<ClientContext> &context, vector<LogicalType> chunk_types);

	vector<LogicalType> chunk_types;
	vector<ColumnDefinition> columns;

public:
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<TableRef> GetTableRef() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
};

} // namespace duckdb
