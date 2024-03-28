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

struct ParquetReadBindData : public TableFunctionData {
    //! The bound names and types
    vector<string> names;
    vector<LogicalType> types;

    //! The metadata provider for this parquet scan
    unique_ptr<ParquetMetadataProvider> metadata_provider;

    atomic<idx_t> chunk_count;

    //! Todo: the initial reader concept should be moved into the metadata provider I think;
    //! bind data itself to remain generic and unspecific
    //! can just ask for the first reader and the caching is abstracted away
    shared_ptr<ParquetReader> initial_reader;

    // The union readers are created (when parquet union_by_name option is on) during binding
    // Those readers can be re-used during ParquetParallelStateNext
    vector<shared_ptr<ParquetReader>> union_readers;

    // These come from the initial_reader, but need to be stored in case the initial_reader is removed by a filter
    // TODO: Move into metadataprovider
    idx_t initial_file_cardinality;
    idx_t initial_file_row_groups;

    ParquetOptions parquet_options;
    MultiFileReaderBindData reader_bind;

    void Initialize(shared_ptr<ParquetReader> reader) {
        initial_reader = std::move(reader);
        initial_file_cardinality = initial_reader->NumRows();
        initial_file_row_groups = initial_reader->NumRowGroups();
        parquet_options = initial_reader->parquet_options;
    }
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
    unique_ptr<mutex[]> file_mutexes;
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

} // namespace duckdb
