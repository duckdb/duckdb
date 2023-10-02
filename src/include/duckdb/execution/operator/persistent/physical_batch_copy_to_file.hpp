//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_batch_copy_to_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/filename_pattern.hpp"

namespace duckdb {

//! Copy the contents of a query into a table
class PhysicalBatchCopyToFile : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::BATCH_COPY_TO_FILE;

public:
	PhysicalBatchCopyToFile(vector<LogicalType> types, CopyFunction function, unique_ptr<FunctionData> bind_data,
	                        idx_t estimated_cardinality);

	CopyFunction function;
	unique_ptr<FunctionData> bind_data;
	string file_path;
	bool use_tmp_file;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	void NextBatch(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p) const override;

	bool RequiresBatchIndex() const override {
		return true;
	}

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}

private:
	void PrepareBatchData(ClientContext &context, GlobalSinkState &gstate_p, idx_t batch_index,
	                      unique_ptr<ColumnDataCollection> collection) const;
	void FlushBatchData(ClientContext &context, GlobalSinkState &gstate_p, idx_t min_index) const;
	SinkFinalizeType FinalFlush(ClientContext &context, GlobalSinkState &gstate_p) const;
};

struct ActiveFlushGuard {
	explicit ActiveFlushGuard(atomic<bool> &bool_value_p) : bool_value(bool_value_p) {
		bool_value = true;
	}
	~ActiveFlushGuard() {
		bool_value = false;
	}

	atomic<bool> &bool_value;
};

} // namespace duckdb
