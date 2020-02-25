//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
class DataTable;

//! Physically delete data from a table
class PhysicalDelete : public PhysicalOperator {
public:
	PhysicalDelete(LogicalOperator &op, TableCatalogEntry &tableref, DataTable &table, idx_t row_id_index)
	    : PhysicalOperator(PhysicalOperatorType::DELETE, op.types), tableref(tableref), table(table),
	      row_id_index(row_id_index) {
	}

	TableCatalogEntry &tableref;
	DataTable &table;
	idx_t row_id_index;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
};

} // namespace duckdb
