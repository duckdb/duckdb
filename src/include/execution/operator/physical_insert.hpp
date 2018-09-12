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
	PhysicalInsert(TableCatalogEntry *table,
	               std::vector<std::unique_ptr<AbstractExpression>> value_list)
	    : PhysicalOperator(PhysicalOperatorType::INSERT),
	      value_list(move(value_list)), table(table) {}

	std::vector<TypeId> GetTypes() override;
	virtual void _GetChunk(ClientContext &context, DataChunk &chunk,
	                       PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

	std::vector<std::unique_ptr<AbstractExpression>> value_list;
	TableCatalogEntry *table;
};

} // namespace duckdb
