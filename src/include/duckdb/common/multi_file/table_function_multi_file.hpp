//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/table_function_multi_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

//! The options of a wrapped single-file table function - the named parameters that are forwarded to it as-is
class TableFunctionFileReaderOptions : public BaseFileReaderOptions {
public:
	named_parameter_map_t named_parameters;
	//! The schema the files are expected to produce - set by COPY, which takes its columns from the target table
	vector<Identifier> expected_names;
	vector<LogicalType> expected_types;
	//! The bind data that determined the schema above, if it came from binding a file of this same scan
	shared_ptr<FunctionData> schema_bind_data;
};

//! Bind data of a multi-file function that wraps a single-file table function
struct TableFunctionMultiFileData : public TableFunctionData {
	TableFunctionFileReaderOptions options;
	//! The per-file cardinality estimate, kept when the readers used to combine the schemas are released
	optional_idx cardinality;

	//! Whether the schema every file is read with is known upfront - because it was combined from several files,
	//! taken from the file the schema was determined on, or given by COPY from the target table
	bool HasExpectedSchema() const {
		return !options.expected_names.empty();
	}
};

//! Settings of a multi-file wrapper - how the wrapped single-file function is exposed as a multi-file function
struct TableFunctionMultiFileSettings {
	//! How the file paths passed to the function are globbed
	FileGlobInput glob_input = FileGlobOptions::DISALLOW_EMPTY;
	//! How the files are referred to in error messages (e.g. "Parquet", "JSON") - defaults to the function name
	string reader_type;
	//! How many files are sampled by default to determine the schema
	idx_t maximum_sample_files = 1;
};

//! The function info of a multi-file wrapper - holds the single-file function that is wrapped
struct TableFunctionMultiFileInfo : public TableFunctionInfo {
	TableFunctionMultiFileInfo(TableFunction function_p, TableFunctionMultiFileSettings settings_p)
	    : function(std::move(function_p)), settings(std::move(settings_p)) {
	}

	TableFunction function;
	TableFunctionMultiFileSettings settings;
};

//! Union data of a wrapped single-file table function
class TableFunctionUnionData : public BaseUnionData {
public:
	explicit TableFunctionUnionData(OpenFileInfo file) : BaseUnionData(std::move(file)) {
	}

	optional_idx cardinality;
	//! The bind data of the wrapped function for this file - used to combine the schemas of several files
	shared_ptr<FunctionData> bind_data;

	optional_idx TryGetCardinalityEstimate() const override {
		return cardinality;
	}
};

//! Reads a single file by binding and executing the wrapped table function over that file
class TableFunctionFileReader : public BaseFileReader {
public:
	TableFunctionFileReader(TableFunction function, OpenFileInfo file, named_parameter_map_t named_parameters,
	                        string reader_type);
	~TableFunctionFileReader() override;

public:
	string GetReaderType() const override;
	shared_ptr<BaseUnionData> GetUnionData(idx_t file_idx) override;
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, const Identifier &name) override;
	void AddVirtualColumn(column_t virtual_column_id) override;
	void PrepareReader(ClientContext &context, GlobalTableFunctionState &gstate) override;
	bool TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                       LocalTableFunctionState &lstate) override;
	AsyncResult Scan(ClientContext &context, GlobalTableFunctionState &gstate, LocalTableFunctionState &lstate,
	                 DataChunk &chunk) override;
	double GetProgressInFile(ClientContext &context) override;
	InsertionOrderPreservingMap<Value> GetMetadata() const override;

	//! Release the resources the given local state holds for the batch it scanned last
	void FinishBatch(ClientContext &context, LocalTableFunctionState &local_state);
	//! Bind the wrapped table function over this file - this sets up the columns of the reader.
	//! When the schema of the scan is known upfront, the file is bound against that schema
	void BindFunction(ClientContext &context, const TableFunctionFileReaderOptions &options);
	//! The cardinality of this file (if the wrapped function can provide one)
	optional_idx GetCardinality() const {
		return cardinality;
	}
	//! The number of threads the wrapped function can use to scan this file
	optional_idx MaxThreads(ClientContext &context);

public:
	//! The wrapped single-file table function
	TableFunction function;
	//! The bind data of the wrapped function for this file
	shared_ptr<FunctionData> bind_data;
	//! The names/types the wrapped function bound to for this file
	vector<Identifier> names;
	vector<LogicalType> types;
	//! The cardinality estimate of the wrapped function for this file (if it has one)
	optional_idx cardinality;

private:
	TableFunctionInitInput GetInitInput() const;
	//! Initialize the global state of the wrapped function (if it has not been initialized yet)
	void InitializeFunctionState(ClientContext &context);

private:
	//! The named parameters that are passed on to the wrapped function
	named_parameter_map_t named_parameters;
	//! How the files are referred to in error messages
	string reader_type;
	//! Guards the initialization of the global state below
	mutex lock;
	//! The global state of the wrapped function - shared by all threads scanning this file
	unique_ptr<GlobalTableFunctionState> global_state;
	//! Set when the wrapped function has no local state - only a single thread can scan the file in that case
	atomic<bool> file_is_assigned;
	//! Set when the wrapped function has emitted its last chunk for this file
	atomic<bool> exhausted;
};

//! A MultiFileReaderInterface that turns a table function reading a single file into a multi-file table function.
//! The wrapped function must accept a single VARCHAR file path as its only positional parameter - all of its named
//! parameters are forwarded to it, and all multi-file options (union_by_name, hive partitioning, filename, ...) are
//! provided by the multi-file framework on top of it.
class TableFunctionMultiFileWrapper : public MultiFileReaderInterface {
public:
	TableFunctionMultiFileWrapper(TableFunction function, TableFunctionMultiFileSettings settings);

public:
	//! Wrap a single-file table function into a multi-file table function
	static TableFunction CreateFunction(TableFunction single_file_function, Identifier name,
	                                    TableFunctionMultiFileSettings settings = TableFunctionMultiFileSettings());
	//! Wrap a single-file table function into a multi-file table function set (VARCHAR and LIST(VARCHAR) variants)
	static TableFunctionSet
	CreateFunctionSet(TableFunction single_file_function, Identifier name,
	                  TableFunctionMultiFileSettings settings = TableFunctionMultiFileSettings());

	//! Binds a COPY ... FROM over the wrapped function - assign this to CopyFunction::copy_from_bind, together with
	//! the wrapped multi-file function as CopyFunction::copy_from_function
	static unique_ptr<FunctionData> MultiFileBindCopy(ClientContext &context, CopyFromFunctionBindInput &input,
	                                                  vector<Identifier> &expected_names,
	                                                  vector<LogicalType> &expected_types);

	//! Only present to satisfy MultiFileFunction - the wrapper builds its interface from the function info instead
	static unique_ptr<MultiFileReaderInterface> CreateInterface(ClientContext &context);

public:
	void InitializeFileOptions(MultiFileOptions &file_options) override;
	unique_ptr<BaseFileReaderOptions> InitializeOptions(ClientContext &context,
	                                                    optional_ptr<TableFunctionInfo> info) override;
	bool ParseCopyOption(ClientContext &context, const Identifier &key, const vector<Value> &values,
	                     BaseFileReaderOptions &options, vector<Identifier> &expected_names,
	                     vector<LogicalType> &expected_types) override;
	bool ParseOption(ClientContext &context, const Identifier &key, const Value &val, MultiFileOptions &file_options,
	                 BaseFileReaderOptions &options) override;
	void FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options,
	                      const vector<Identifier> &expected_names, const vector<LogicalType> &expected_types) override;
	unique_ptr<TableFunctionData> InitializeBindData(MultiFileBindData &multi_file_data,
	                                                 unique_ptr<BaseFileReaderOptions> options) override;
	optional_idx MaxThreads(ClientContext &context, const MultiFileBindData &bind_data,
	                        const MultiFileGlobalState &global_state, FileExpandResult expand_result) override;
	void BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<Identifier> &names,
	                MultiFileBindData &bind_data) override;
	void CombineSchemas(ClientContext &context, const vector<shared_ptr<BaseUnionData>> &union_data,
	                    vector<LogicalType> &return_types, vector<Identifier> &names) override;
	void FinalizeBindData(MultiFileBindData &multi_file_data) override;
	unique_ptr<GlobalTableFunctionState> InitializeGlobalState(ClientContext &context, MultiFileBindData &bind_data,
	                                                           MultiFileGlobalState &global_state) override;
	unique_ptr<LocalTableFunctionState> InitializeLocalState(ClientContext &context,
	                                                         GlobalTableFunctionState &global_state) override;
	void FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
	                   LocalTableFunctionState &local_state) override;
	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                        BaseUnionData &union_data, const MultiFileBindData &bind_data) override;
	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                        const OpenFileInfo &file, idx_t file_idx,
	                                        const MultiFileBindData &bind_data) override;
	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, const OpenFileInfo &file,
	                                        BaseFileReaderOptions &options,
	                                        const MultiFileOptions &file_options) override;
	unique_ptr<NodeStatistics> GetCardinality(ClientContext &context, const MultiFileBindData &bind_data,
	                                          idx_t file_count) override;
	unique_ptr<MultiFileReaderInterface> Copy() override;
	FileGlobInput GetGlobInput() override;

private:
	//! Parse a named parameter of the wrapped function - returns false if the function has no such parameter
	bool ParseNamedParameter(const Identifier &key, const Value &val, TableFunctionFileReaderOptions &options) const;
	//! Release the per-file bind data that is only needed while combining schemas
	static void ReleaseBindData(const vector<shared_ptr<BaseUnionData>> &union_data);

public:
	//! The single-file table function that is wrapped
	TableFunction function;
	//! How the wrapped function is exposed as a multi-file function
	TableFunctionMultiFileSettings settings;
	//! The schema obtained by combining the schemas of the files, and the bind data describing it (if any).
	//! "schema_combined" distinguishes "no combining happened" from "combining produced an empty schema"
	bool schema_combined = false;
	vector<Identifier> combined_names;
	vector<LogicalType> combined_types;
	shared_ptr<FunctionData> combined_bind_data;
};

} // namespace duckdb
