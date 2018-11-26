//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/physical_insert.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! Physically insert a set of data into a table
class PhysicalInsert : public PhysicalOperator {
  public:
	PhysicalInsert(
	    TableCatalogEntry *table,
	    std::vector<std::vector<std::unique_ptr<Expression>>> insert_values,
	    std::vector<int> column_index_map)
	    : PhysicalOperator(PhysicalOperatorType::INSERT),
	      column_index_map(column_index_map),
	      insert_values(move(insert_values)), table(table) {
	}

	std::vector<std::string> GetNames() override;
	std::vector<TypeId> GetTypes() override;

	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

	std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

	std::vector<int> column_index_map;
	std::vector<std::vector<std::unique_ptr<Expression>>> insert_values;
	TableCatalogEntry *table;
};

} // namespace duckdb
