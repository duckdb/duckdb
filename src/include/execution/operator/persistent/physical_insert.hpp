//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/persistent/physical_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! Physically insert a set of data into a table
class PhysicalInsert : public PhysicalOperator {
public:
	PhysicalInsert(LogicalOperator &op, TableCatalogEntry *table, vector<vector<unique_ptr<Expression>>> insert_values,
	               vector<int> column_index_map)
	    : PhysicalOperator(PhysicalOperatorType::INSERT, op.types), column_index_map(column_index_map),
	      insert_values(move(insert_values)), table(table) {
	}

	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	vector<int> column_index_map;
	vector<vector<unique_ptr<Expression>>> insert_values;
	TableCatalogEntry *table;
};

} // namespace duckdb
