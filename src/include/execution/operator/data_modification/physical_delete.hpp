//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/data_modification/physical_delete.hpp
//
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! Physically delete data from a table
class PhysicalDelete : public PhysicalOperator {
  public:
	PhysicalDelete(LogicalOperator &op, TableCatalogEntry &tableref, DataTable &table)
	    : PhysicalOperator(PhysicalOperatorType::DELETE, op.types), tableref(tableref),
	      table(table) {
	}

	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

	std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

	TableCatalogEntry &tableref;
	DataTable &table;
};

} // namespace duckdb
