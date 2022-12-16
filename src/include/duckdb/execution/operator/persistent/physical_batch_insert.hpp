//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_batch_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/persistent/physical_insert.hpp"

namespace duckdb {

class PhysicalBatchInsert : public PhysicalOperator {
public:
	//! INSERT INTO
	PhysicalBatchInsert(vector<LogicalType> types, TableCatalogEntry *table,
	                    physical_index_vector_t<idx_t> column_index_map, vector<unique_ptr<Expression>> bound_defaults,
	                    idx_t estimated_cardinality);
	//! CREATE TABLE AS
	PhysicalBatchInsert(LogicalOperator &op, SchemaCatalogEntry *schema, unique_ptr<BoundCreateTableInfo> info,
	                    idx_t estimated_cardinality);

	//! The map from insert column index to table column index
	physical_index_vector_t<idx_t> column_index_map;
	//! The table to insert into
	TableCatalogEntry *insert_table;
	//! The insert types
	vector<LogicalType> insert_types;
	//! The default expressions of the columns for which no value is provided
	vector<unique_ptr<Expression>> bound_defaults;
	//! Table schema, in case of CREATE TABLE AS
	SchemaCatalogEntry *schema;
	//! Create table info, in case of CREATE TABLE AS
	unique_ptr<BoundCreateTableInfo> info;
	// Which action to perform on conflict
	InsertConflictActionType action_type;

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
	void Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          GlobalSinkState &gstate) const override;

	bool RequiresBatchIndex() const override {
		return true;
	}

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}
};

} // namespace duckdb
