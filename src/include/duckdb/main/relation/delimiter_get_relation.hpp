//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/delimiter_get_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"

namespace duckdb {

class DelimiterGetRelation : public Relation {
public:
	DUCKDB_API DelimiterGetRelation(vector<LogicalType> chunk_types);

	vector<LogicalType> chunk_types;

public:
	unique_ptr<QueryNode> GetQueryNode() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;

	unique_ptr<TableRef> GetTableRef() override;
};

} // namespace duckdb
