//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_sink.hpp"

namespace duckdb {
class DataTable;

//! Physically delete data from a table
class PhysicalDelete : public PhysicalSink {
public:
	PhysicalDelete(vector<LogicalType> types, TableCatalogEntry &tableref, DataTable &table, idx_t row_id_index,
	               idx_t estimated_cardinality)
	    : PhysicalSink(PhysicalOperatorType::DELETE_OPERATOR, move(types), estimated_cardinality), tableref(tableref),
	      table(table), row_id_index(row_id_index) {
	}

	TableCatalogEntry &tableref;
	DataTable &table;
	idx_t row_id_index;

public:
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;
	void Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
	          DataChunk &input) const override;

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
};

} // namespace duckdb
