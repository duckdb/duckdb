//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class DataTable;

//! Physically update data in a table
class PhysicalUpdate : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::UPDATE;

public:
	PhysicalUpdate(vector<LogicalType> types, TableCatalogEntry &tableref, DataTable &table,
	               vector<PhysicalIndex> columns, vector<unique_ptr<Expression>> expressions,
	               vector<unique_ptr<Expression>> bound_defaults, idx_t estimated_cardinality, bool return_chunk);

	TableCatalogEntry &tableref;
	DataTable &table;
	vector<PhysicalIndex> columns;
	vector<unique_ptr<Expression>> expressions;
	vector<unique_ptr<Expression>> bound_defaults;
	bool update_is_del_and_insert;
	//! If the returning statement is present, return the whole chunk
	bool return_chunk;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}
};

} // namespace duckdb
