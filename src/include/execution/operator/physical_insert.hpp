//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/physical_insert.hpp
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
	PhysicalInsert(std::shared_ptr<TableCatalogEntry> table,
	               std::vector<std::unique_ptr<AbstractExpression>> value_list)
	    : PhysicalOperator(PhysicalOperatorType::INSERT),
	      value_list(move(value_list)), table(table) {}

	virtual void InitializeChunk(DataChunk &chunk) override;
	virtual void GetChunk(DataChunk &chunk,
	                      PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

	std::vector<std::unique_ptr<AbstractExpression>> value_list;
	std::shared_ptr<TableCatalogEntry> table;
};

} // namespace duckdb
