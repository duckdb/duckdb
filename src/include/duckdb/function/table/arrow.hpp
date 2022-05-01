//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/arrow.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/parallel/parallel_state.hpp"
#include "duckdb/common/arrow_wrapper.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/thread.hpp"
#include <map>
#include <condition_variable>

namespace duckdb {
//===--------------------------------------------------------------------===//
// Arrow Variable Size Types
//===--------------------------------------------------------------------===//
enum class ArrowVariableSizeType : uint8_t { FIXED_SIZE = 0, NORMAL = 1, SUPER_SIZE = 2 };

//===--------------------------------------------------------------------===//
// Arrow Time/Date Types
//===--------------------------------------------------------------------===//
enum class ArrowDateTimeType : uint8_t {
	MILLISECONDS = 0,
	MICROSECONDS = 1,
	NANOSECONDS = 2,
	SECONDS = 3,
	DAYS = 4,
	MONTHS = 5
};
struct ArrowConvertData {
	ArrowConvertData(LogicalType type) : dictionary_type(type) {};
	ArrowConvertData() {};
	//! Hold type of dictionary
	LogicalType dictionary_type;
	//! If its a variable size type (e.g., strings, blobs, lists) holds which type it is
	vector<std::pair<ArrowVariableSizeType, idx_t>> variable_sz_type;
	//! If this is a date/time holds its precision
	vector<ArrowDateTimeType> date_time_precision;
};

struct ArrowScanFunctionData : public TableFunctionData {
#ifndef DUCKDB_NO_THREADS

	ArrowScanFunctionData(idx_t rows_per_thread_p,
	                      unique_ptr<ArrowArrayStreamWrapper> (*scanner_producer_p)(
	                          uintptr_t stream_factory_ptr,
	                          std::pair<std::unordered_map<idx_t, string>, std::vector<string>> &project_columns,
	                          TableFilterCollection *filters),
	                      uintptr_t stream_factory_ptr_p, std::thread::id thread_id_p)
	    : lines_read(0), rows_per_thread(rows_per_thread_p), stream_factory_ptr(stream_factory_ptr_p),
	      scanner_producer(scanner_producer_p), number_of_rows(0), thread_id(thread_id_p) {
	}
#endif

	ArrowScanFunctionData(idx_t rows_per_thread_p,
	                      unique_ptr<ArrowArrayStreamWrapper> (*scanner_producer_p)(
	                          uintptr_t stream_factory_ptr,
	                          std::pair<std::unordered_map<idx_t, string>, std::vector<string>> &project_columns,
	                          TableFilterCollection *filters),
	                      uintptr_t stream_factory_ptr_p)
	    : lines_read(0), rows_per_thread(rows_per_thread_p), stream_factory_ptr(stream_factory_ptr_p),
	      scanner_producer(scanner_producer_p), number_of_rows(0) {
	}
	//! This holds the original list type (col_idx, [ArrowListType,size])
	std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> arrow_convert_data;
	std::atomic<idx_t> lines_read;
	ArrowSchemaWrapper schema_root;
	idx_t rows_per_thread;
	//! Pointer to the scanner factory
	uintptr_t stream_factory_ptr;
	//! Pointer to the scanner factory produce
	unique_ptr<ArrowArrayStreamWrapper> (*scanner_producer)(
	    uintptr_t stream_factory_ptr,
	    std::pair<std::unordered_map<idx_t, string>, std::vector<string>> &project_columns,
	    TableFilterCollection *filters);
	//! Number of rows (Used in cardinality and progress bar)
	int64_t number_of_rows;
#ifndef DUCKDB_NO_THREADS
	// Thread that made first call in the binder
	std::thread::id thread_id;
#endif
};

struct ArrowScanState : public FunctionOperatorData {
	explicit ArrowScanState(unique_ptr<ArrowArrayWrapper> current_chunk) : chunk(move(current_chunk)) {
	}
	unique_ptr<ArrowArrayStreamWrapper> stream;
	shared_ptr<ArrowArrayWrapper> chunk;
	idx_t chunk_offset = 0;
	vector<column_t> column_ids;
	//! Store child vectors for Arrow Dictionary Vectors (col-idx,vector)
	unordered_map<idx_t, unique_ptr<Vector>> arrow_dictionary_vectors;
	TableFilterCollection *filters = nullptr;
};

struct ParallelArrowScanState : public ParallelState {
	ParallelArrowScanState() {
	}
	unique_ptr<ArrowArrayStreamWrapper> stream;
	std::mutex main_mutex;
	bool ready = false;
};

struct ArrowTableFunction {
public:
	static void RegisterFunction(BuiltinFunctions &set);

private:
	//! Binds an arrow table
	static unique_ptr<FunctionData> ArrowScanBind(ClientContext &context, TableFunctionBindInput &input,
	                                              vector<LogicalType> &return_types, vector<string> &names);
	//! Actual conversion from Arrow to DuckDB
	static void ArrowToDuckDB(ArrowScanState &scan_state,
	                          std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data,
	                          DataChunk &output, idx_t start);

	//! -----Single Thread Functions:-----
	//! Initialize Single Thread Scan
	static unique_ptr<FunctionOperatorData> ArrowScanInit(ClientContext &context, const FunctionData *bind_data,
	                                                      const vector<column_t> &column_ids,
	                                                      TableFilterCollection *filters);

	//! Scan Function for Single Thread Execution
	static void ArrowScanFunction(ClientContext &context, const FunctionData *bind_data,
	                              FunctionOperatorData *operator_state, DataChunk &output);

	//! -----Multi Thread Functions:-----
	//! Initialize Parallel State
	static unique_ptr<ParallelState> ArrowScanInitParallelState(ClientContext &context, const FunctionData *bind_data_p,
	                                                            const vector<column_t> &column_ids,
	                                                            TableFilterCollection *filters);
	//! Initialize Parallel Scans
	static unique_ptr<FunctionOperatorData> ArrowScanParallelInit(ClientContext &context,
	                                                              const FunctionData *bind_data_p, ParallelState *state,
	                                                              const vector<column_t> &column_ids,
	                                                              TableFilterCollection *filters);
	//! Defines Maximum Number of Threads
	static idx_t ArrowScanMaxThreads(ClientContext &context, const FunctionData *bind_data_p);
	//! Scan Function for Parallel Execution
	static void ArrowScanFunctionParallel(ClientContext &context, const FunctionData *bind_data,
	                                      FunctionOperatorData *operator_state, DataChunk &output,
	                                      ParallelState *parallel_state_p);
	//! Get next chunk for the running thread
	static bool ArrowScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
	                                       FunctionOperatorData *operator_state, ParallelState *parallel_state_p);

	//! -----Utility Functions:-----
	//! Gets Arrow Table's Cardinality
	static unique_ptr<NodeStatistics> ArrowScanCardinality(ClientContext &context, const FunctionData *bind_data);
	//! Gets the progress on the table scan, used for Progress Bars
	static double ArrowProgress(ClientContext &context, const FunctionData *bind_data_p);
};

} // namespace duckdb
