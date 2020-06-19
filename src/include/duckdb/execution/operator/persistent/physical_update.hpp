//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
class DataTable;

//! Physically update data in a table
class PhysicalUpdate : public PhysicalSink {
public:
	PhysicalUpdate(LogicalOperator &op, TableCatalogEntry &tableref, DataTable &table, vector<column_t> columns,
	               vector<unique_ptr<Expression>> expressions, vector<unique_ptr<Expression>> bound_defaults)
	    : PhysicalSink(PhysicalOperatorType::UPDATE, op.types), tableref(tableref), table(table), columns(columns),
	      expressions(move(expressions)), bound_defaults(move(bound_defaults)) {
	}

	TableCatalogEntry &tableref;
	DataTable &table;
	vector<column_t> columns;
	vector<unique_ptr<Expression>> expressions;
	vector<unique_ptr<Expression>> bound_defaults;
	bool is_index_update;

public:
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ClientContext &context) override;
	void Sink(ClientContext &context, GlobalOperatorState &state, LocalSinkState &lstate, DataChunk &input) override;

	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
};

} // namespace duckdb
