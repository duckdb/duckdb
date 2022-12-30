//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"

#include "duckdb/storage/data_table.hpp"

#include <fstream>

namespace duckdb {

//! Physical CREATE (UNIQUE) INDEX statement
class PhysicalCreateIndex : public PhysicalOperator {
public:
	PhysicalCreateIndex(LogicalOperator &op, TableCatalogEntry &table, vector<column_t> column_ids,
	                    unique_ptr<CreateIndexInfo> info, vector<unique_ptr<Expression>> unbound_expressions,
	                    idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::CREATE_INDEX, op.types, estimated_cardinality), table(table),
	      info(std::move(info)), unbound_expressions(move(unbound_expressions)) {

		// convert virtual column ids to storage column ids
		for (auto &column_id : column_ids) {
			storage_ids.push_back(table.columns.LogicalToPhysical(LogicalIndex(column_id)).index);
		}
	}

	//! The table to create the index for
	TableCatalogEntry &table;
	//! The list of column IDs required for the index
	vector<column_t> storage_ids;
	//! Info for index creation
	unique_ptr<CreateIndexInfo> info;
	//! Unbound expressions to be used in the optimizer
	vector<unique_ptr<Expression>> unbound_expressions;

public:
	//! Source interface, NOP for this operator
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

public:
	//! Sink interface, thread-local sink states
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	//! Sink interface, global sink state
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	SinkResultType Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
	                    DataChunk &input) const override;
	void Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          GlobalSinkState &gstate) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}
};
} // namespace duckdb
