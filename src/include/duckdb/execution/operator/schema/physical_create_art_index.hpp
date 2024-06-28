//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_art_index.hpp
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
class DuckTableEntry;

//! Physical CREATE (UNIQUE) INDEX statement
class PhysicalCreateARTIndex : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CREATE_INDEX;

public:
	PhysicalCreateARTIndex(LogicalOperator &op, TableCatalogEntry &table, const vector<column_t> &column_ids,
	                       unique_ptr<CreateIndexInfo> info, vector<unique_ptr<Expression>> unbound_expressions,
	                       idx_t estimated_cardinality, const bool sorted);

	//! The table to create the index for
	DuckTableEntry &table;
	//! The list of column IDs required for the index
	vector<column_t> storage_ids;
	//! Info for index creation
	unique_ptr<CreateIndexInfo> info;
	//! Unbound expressions to be used in the optimizer
	vector<unique_ptr<Expression>> unbound_expressions;
	//! Whether the pipeline sorts the data prior to index creation
	const bool sorted;

public:
	//! Source interface, NOP for this operator
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	//! Sink interface, thread-local sink states
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	//! Sink interface, global sink state
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	//! Sink for unsorted data: insert iteratively
	SinkResultType SinkUnsorted(Vector &row_identifiers, OperatorSinkInput &input) const;
	//! Sink for sorted data: build + merge
	SinkResultType SinkSorted(Vector &row_identifiers, OperatorSinkInput &input) const;

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}
};
} // namespace duckdb
