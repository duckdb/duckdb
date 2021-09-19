//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! Physically insert a set of data into a table
class PhysicalInsert : public PhysicalOperator {
public:
	PhysicalInsert(vector<LogicalType> types, TableCatalogEntry *table, vector<idx_t> column_index_map,
	               vector<unique_ptr<Expression>> bound_defaults, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::INSERT, move(types), estimated_cardinality),
	      column_index_map(std::move(column_index_map)), table(table), bound_defaults(move(bound_defaults)) {
	}

	vector<idx_t> column_index_map;
	TableCatalogEntry *table;
	vector<unique_ptr<Expression>> bound_defaults;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate, LocalSourceState &lstate) const override;

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
		return true;
	}
};

} // namespace duckdb
