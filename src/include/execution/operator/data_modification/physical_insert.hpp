//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/data_modification/physical_insert.hpp
//
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! Physically insert a set of data into a table
class PhysicalInsert : public PhysicalOperator {
  public:
	PhysicalInsert(
        LogicalOperator &op,
	    TableCatalogEntry *table,
	    std::vector<std::vector<std::unique_ptr<Expression>>> insert_values,
	    std::vector<int> column_index_map)
	    : PhysicalOperator(PhysicalOperatorType::INSERT, op.types),
	      column_index_map(column_index_map),
	      insert_values(move(insert_values)), table(table) {
	}

	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

	std::vector<int> column_index_map;
	std::vector<std::vector<std::unique_ptr<Expression>>> insert_values;
	TableCatalogEntry *table;
};

} // namespace duckdb
