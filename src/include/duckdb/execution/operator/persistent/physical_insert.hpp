//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! Physically insert a set of data into a table
class PhysicalInsert : public PhysicalOperator {
public:
	PhysicalInsert(vector<LogicalType> types, TableCatalogEntry *table, vector<idx_t> column_index_map,
	               vector<unique_ptr<Expression>> bound_defaults, idx_t estimated_cardinality, bool return_chunk);

	//! The map from insert column index to table column index
	vector<idx_t> column_index_map;
	//! The table to insert into
	TableCatalogEntry *table;
	//! The default expressions of the columns for which no value is provided
	vector<unique_ptr<Expression>> bound_defaults;
	//! If the returning statement is present, return the whole chunk
	bool return_chunk;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	void Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	                    DataChunk &input) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}
};

} // namespace duckdb
