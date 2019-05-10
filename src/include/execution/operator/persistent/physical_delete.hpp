//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/persistent/physical_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! Physically delete data from a table
class PhysicalDelete : public PhysicalOperator {
public:
	PhysicalDelete(LogicalOperator &op, TableCatalogEntry &tableref, DataTable &table, uint64_t row_id_index)
	    : PhysicalOperator(PhysicalOperatorType::DELETE, op.types), tableref(tableref), table(table),
	      row_id_index(row_id_index) {
	}

	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	TableCatalogEntry &tableref;
	DataTable &table;
	uint64_t row_id_index;
};

} // namespace duckdb
