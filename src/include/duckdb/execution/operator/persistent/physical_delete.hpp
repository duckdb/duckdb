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
	PhysicalDelete(vector<LogicalType> types, TableCatalogEntry &tableref, DataTable &table, idx_t row_id_index,
	               idx_t estimated_cardinality, bool return_chunk)
	    : PhysicalOperator(PhysicalOperatorType::DELETE_OPERATOR, std::move(types), estimated_cardinality),
	      tableref(tableref), table(table), row_id_index(row_id_index), return_chunk(return_chunk) {
	}

	TableCatalogEntry &tableref;
	DataTable &table;
	idx_t row_id_index;
	bool return_chunk;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	                    DataChunk &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}
};

} // namespace duckdb
