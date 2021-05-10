//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_sink.hpp"

namespace duckdb {

//! Physically insert a set of data into a table
class PhysicalInsert : public PhysicalSink {
public:
	PhysicalInsert(vector<LogicalType> types, TableCatalogEntry *table, vector<idx_t> column_index_map,
	               vector<unique_ptr<Expression>> bound_defaults, idx_t estimated_cardinality)
	    : PhysicalSink(PhysicalOperatorType::INSERT, move(types), estimated_cardinality),
	      column_index_map(std::move(column_index_map)), table(table), bound_defaults(move(bound_defaults)) {
	}

	vector<idx_t> column_index_map;
	TableCatalogEntry *table;
	vector<unique_ptr<Expression>> bound_defaults;

public:
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) override;
	void Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
	          DataChunk &input) const override;

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const override;
};

} // namespace duckdb
