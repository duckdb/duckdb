//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_copy_to_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/copy_overwrite_mode.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/filename_pattern.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/storage/storage_lock.hpp"

namespace duckdb {

struct GlobalFileState;
enum class PhysicalCopyToFilePhase : uint8_t;

struct CopyToFileInfo {
	explicit CopyToFileInfo(string file_path_p) : file_path(std::move(file_path_p)) {
	}

	string file_path;
	unique_ptr<CopyFunctionFileStatistics> file_stats;
	Value partition_keys;
};

//! Copy the contents of a query into a table
class PhysicalCopyToFile : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::COPY_TO_FILE;

public:
	PhysicalCopyToFile(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function,
	                   unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality);

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;

public:
	static string GetTrimmedPath(ClientContext &context, const string &file_path);
	static void MoveTmpFile(ClientContext &context, const string &tmp_file_path);
	static string GetNonTmpFile(ClientContext &context, const string &tmp_file_path);
	static void ReturnStatistics(DataChunk &chunk, idx_t row_idx, CopyToFileInfo &written_file_info);

	bool Rotate() const;
	bool RotateNow(GlobalFileState &global_state) const;
	void FlushBatch(ClientContext &context, GlobalSinkState &gstate_p, unique_ptr<GlobalFileState> &file_state_ptr,
	                const std::function<unique_ptr<GlobalFileState>()> &create_file_state_fun,
	                unique_ptr<LocalFunctionData> &lstate, unique_ptr<ColumnDataCollection> batch,
	                PhysicalCopyToFilePhase phase) const;

public:
	//===--------------------------------------------------------------------===//
	// Sink Interface
	//===--------------------------------------------------------------------===//
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

public:
	//===--------------------------------------------------------------------===//
	// Source Interface
	//===--------------------------------------------------------------------===//
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	//! Function and associated bind data
	CopyFunction function;
	unique_ptr<FunctionData> bind_data;

	//! Names and types going into the file(s)
	vector<string> names;
	vector<LogicalType> expected_types;

	//! Where to write the file
	string file_path;
	//! Pattern to use for the file names
	FilenamePattern filename_pattern;
	//! File extension, e.g., "parquet"
	string file_extension;

	//! Whether we can write in parallel
	bool parallel;
	//! Whether each threads creates their own output file
	bool per_thread_output;

	//! Fine-grained control over writes
	optional_idx batch_size;
	optional_idx batch_size_bytes;
	optional_idx batches_per_file;
	optional_idx file_size_bytes;

	//! Whether to write to a temp file before writing to the file at "file_path"
	bool use_tmp_file;
	//! What to do when the "file_path" conflicts with an existing file
	CopyOverwriteMode overwrite_mode;
	//! What to return, e.g., number of written rows
	CopyFunctionReturnType return_type;
	//! Whether to write an empty file if there was no data
	bool write_empty_file;

	//! Partitioning-related
	vector<idx_t> partition_columns;
	bool partition_output;
	bool write_partition_columns;
	bool hive_file_pattern;
};

} // namespace duckdb
