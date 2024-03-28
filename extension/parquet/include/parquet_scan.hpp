//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parquet_reader.hpp"

namespace duckdb {
class ParquetMetadataProvider;

struct ParquetReadBindData : public TableFunctionData {
    //! The bound names and types TODO: when passing the schema, we also have this information in the ParquetOptions
    vector<string> names;
    vector<LogicalType> types;

    //! The metadata provider for this parquet scan
    unique_ptr<ParquetMetadataProvider> metadata_provider;

	//! Used for counting chunks for progress estimation
    atomic<idx_t> chunk_count;

	//! The parquet options for this scan
	//! TODO: these used to be initialized with the options from the first opened reader, that is now gone, I should check
	//!       if that needs to be restored?
	ParquetOptions parquet_options;

	//! The MultifileReader specific bind data
	MultiFileReaderBindData reader_bind;
};

enum class ParquetFileState : uint8_t { UNOPENED, OPENING, OPEN, CLOSED };

struct ParquetReadLocalState : public LocalTableFunctionState {
    shared_ptr<ParquetReader> reader;
    ParquetReaderScanState scan_state;
    bool is_parallel;
    idx_t batch_index;
    idx_t file_index;
    //! The DataChunk containing all read columns (even filter columns that are immediately removed)
    DataChunk all_columns;
};

struct ParquetReadGlobalState : public GlobalTableFunctionState {
    mutex lock;

    //! The initial reader from the bind phase
    shared_ptr<ParquetReader> initial_reader;
    //! Currently opened readers
    vector<shared_ptr<ParquetReader>> readers;
    //! Flag to indicate a file is being opened
    vector<ParquetFileState> file_states;
    //! Mutexes to wait for a file that is currently being opened
    vector<unique_ptr<mutex>> file_mutexes;
    //! Signal to other threads that a file failed to open, letting every thread abort.
    bool error_opening_file = false;

    //! Index of file currently up for scanning
    atomic<idx_t> file_index;
    //! Index of row group within file currently up for scanning
    idx_t row_group_index;
    //! Batch index of the next row group to be scanned
    idx_t batch_index;

    idx_t max_threads;
    vector<idx_t> projection_ids;
    vector<LogicalType> scanned_types;
    vector<column_t> column_ids;
    TableFilterSet *filters;

    idx_t MaxThreads() const override {
        return max_threads;
    }

    bool CanRemoveFilterColumns() const {
        return !projection_ids.empty();
    }
};

class ParquetScanFunction {

//! Core functions for creating parquet scans
public:
    //! Get the default DuckDB Parquet scans
    static TableFunctionSet GetFunctionSet();

    //! Create a parquet scan
    static TableFunction CreateParquetScan(const string &name, table_function_bind_t bind_function,
                                                 table_function_serialize_t serialize, table_function_deserialize_t deserialize);

//! Functions related to the current filelist-based scan TODO: move out of this class
public:
    static unique_ptr<FunctionData> ParquetScanBindInternal(ClientContext &context, vector<string> files,
                                                            vector<LogicalType> &return_types, vector<string> &names,
                                                            ParquetOptions parquet_options);
    static unique_ptr<FunctionData> ParquetReadBind(ClientContext &context, CopyInfo &info,
                                                    vector<string> &expected_names,
                                                    vector<LogicalType> &expected_types);
    static unique_ptr<FunctionData> ParquetScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names);
    static void ParquetScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                     const TableFunction &function);
    static unique_ptr<FunctionData> ParquetScanDeserialize(Deserializer &deserializer, TableFunction &function);

    static unique_ptr<BaseStatistics> ParquetScanStats(ClientContext &context, const FunctionData *bind_data_p,
                                                       column_t column_index);

//! Shared methods between all parquet scans
protected:
    //! Initialize local state
    static unique_ptr<LocalTableFunctionState> ParquetScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                                    GlobalTableFunctionState *gstate_p);
    //! Initialize global state
    static unique_ptr<GlobalTableFunctionState> ParquetScanInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input);
    //! Scan a chunk
    static void ParquetScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);

	//! Initialize a reader, passing through the pushed-down filters, projections etc.
	static void InitializeParquetReader(ParquetReader &reader, const ParquetReadBindData &bind_data,
	                             const vector<column_t> &global_column_ids,
	                             optional_ptr<TableFilterSet> table_filters, ClientContext &context);

    static double ParquetProgress(ClientContext &context, const FunctionData *bind_data_p,
                                  const GlobalTableFunctionState *global_state);
    static idx_t ParquetScanGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                                          LocalTableFunctionState *local_state,
                                          GlobalTableFunctionState *global_state);
    static unique_ptr<NodeStatistics> ParquetCardinality(ClientContext &context, const FunctionData *bind_data);
    static idx_t ParquetScanMaxThreads(ClientContext &context, const FunctionData *bind_data);
    static bool ParquetParallelStateNext(ClientContext &context, const ParquetReadBindData &bind_data,
                                         ParquetReadLocalState &scan_data, ParquetReadGlobalState &parallel_state);
    static void ParquetComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                             vector<unique_ptr<Expression>> &filters);
    //! Wait for a file to become available. Parallel lock should be locked when calling.
    static void WaitForFile(idx_t file_index, ParquetReadGlobalState &parallel_state,
                            unique_lock<mutex> &parallel_lock);
    //! Helper function that try to start opening a next file. Parallel lock should be locked when calling.
    static bool TryOpenNextFile(ClientContext &context, const ParquetReadBindData &bind_data,
                                ParquetReadLocalState &scan_data, ParquetReadGlobalState &parallel_state,
                                unique_lock<mutex> &parallel_lock);
};


//! This class manages the Metadata required for a parquet scan, the parquet scan can then use these
//! The goal of having this here is to have a clear API against which to implement stuff like:
//! - DeltaTableMetaDataProvider
//! - GlobMetaDataProvider (for efficient globbing, especially with filters)
//! - MultiFileMetaDataProvider
class ParquetMetadataProvider {
public:
	ParquetMetadataProvider(const vector<string>& files) : files(files){
	}

	//! Core API; to be changed into abstract base class and extended by:
	//! - DeltaTableMetadataProvider ( uses delta metadata )
	//! - GlobMetadataProvider ( uses a glob pattern that is lazily expanded as needed )
	//! - FileListMetadataProvider ( uses a list of files )
public:
	//! DEPRECATED: parquet reader should not try to read the whole file list anymore, any usages of this should be removed
	//! TODO: disable this and fix any code that depends on it
	const vector<string>& GetFiles() {
		return files;
	}

	//! Whether the scan can produce data at all. (e.g. filter pushdown can eliminate every tuple)
	bool HaveData();
	//! Return the initial reader (could be nullptr)
	shared_ptr<ParquetReader> GetInitialReader();
	//! Return all currently initialized readers (could be empty if the MetadataProvider does not open any parquet files)
	vector<shared_ptr<ParquetReader>> GetInitializedReaders();

	//! This could be used for reads that require knowing the filenames of 1 or more files. TODO: remove?
	string GetFile(idx_t i);

	//! This would be an optional call to be implemented by the HiveFilteredGlob; necessary for
	const string GetAnyFile();

	//! Returns the deletion vector for a fil
	string GetDeletionVector(string);

	//! TODO: add a function call that returns the next parquet reader to be used in parquet reading

	//! TODO: the bind should probably not go in here; we can just do a separate bind function for delta which will
	//! ensure the correct ParquetMetadataProvider is returned;
	void Bind(ClientContext &context, TableFunctionBindInput &input,
	          vector<LogicalType> &return_types, vector<string> &names);

	//! Pushes the filters down into the ParquetScanMetaDataProvider; this ensures when GetFile() is called, the
	//! MetaDataProvider can use the filters to ensure only files are passed through that match the filters
	void FilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
	                    vector<unique_ptr<Expression>> &filters);

	//! Return the statistics of a column
	unique_ptr<BaseStatistics> ParquetScanStats(ClientContext &context, const FunctionData *bind_data_p,
	                                            column_t column_index);
	//! Returns the progress of the current scan
	double ParquetProgress(ClientContext &context, const FunctionData *bind_data_p,
	                       const GlobalTableFunctionState *global_state);
	//! Returns the cardinality
	unique_ptr<NodeStatistics> ParquetCardinality(ClientContext &context, const FunctionData *bind_data);
	//! Max Threads to be scanned with
	idx_t ParquetScanMaxThreads(ClientContext &context, const FunctionData *bind_data);

//! These calls are specific to the current MultiFileMetaDataProvider
public:
	void Initialize(shared_ptr<ParquetReader> reader) {
		initial_reader = std::move(reader);
		initial_file_cardinality = initial_reader->NumRows();
		initial_file_row_groups = initial_reader->NumRowGroups();
	}
protected:
	// The set of files to be scanned
	vector<string> files;

	// These come from the initial_reader, but need to be stored in case the initial_reader is removed by a filter
	idx_t initial_file_cardinality;
	idx_t initial_file_row_groups;

public: // todo make private
	shared_ptr<ParquetReader> initial_reader;
	// The union readers are created (when parquet union_by_name option is on) during binding
	// Those readers can be re-used during ParquetParallelStateNext
	vector<shared_ptr<ParquetReader>> union_readers;
};

// TODO: We can also abstract the other way around:
// Where the internals of the parquet scan are grouped together and then we can reuse
// just that logic and ignore the rest. That keeps things more flexible probably
// NOPE -> this would not allow use to implement the hive optimization.


} // namespace duckdb
