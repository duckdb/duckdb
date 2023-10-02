//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_copy_to_file.hpp
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
class PhysicalCopyToFile : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::COPY_TO_FILE;

public:
	PhysicalCopyToFile(vector<LogicalType> types, CopyFunction function, unique_ptr<FunctionData> bind_data,
	                   idx_t estimated_cardinality);

	CopyFunction function;
	unique_ptr<FunctionData> bind_data;
	string file_path;
	bool use_tmp_file;
	FilenamePattern filename_pattern;
	bool overwrite_or_ignore;
	bool parallel;
	bool per_thread_output;

	bool partition_output;
	vector<idx_t> partition_columns;
	vector<string> names;
	vector<LogicalType> expected_types;

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

	bool IsSink() const override {
		return true;
	}

	bool SinkOrderDependent() const override {
		return true;
	}

	bool ParallelSink() const override {
		return per_thread_output || partition_output || parallel;
	}

	static void MoveTmpFile(ClientContext &context, const string &tmp_file_path);
};
} // namespace duckdb
