//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/physical_delete.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! Physically delete data from a table
class PhysicalDelete : public PhysicalOperator {
  public:
	PhysicalDelete(DataTable &table)
	    : PhysicalOperator(PhysicalOperatorType::DELETE), table(table) {
	}

	std::vector<std::string> GetNames() override;
	std::vector<TypeId> GetTypes() override;

	virtual void _GetChunk(ClientContext &context, DataChunk &chunk,
	                       PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

	DataTable &table;
};

} // namespace duckdb
