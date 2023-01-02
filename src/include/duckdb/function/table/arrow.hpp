//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/arrow.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/function/built_in_functions.hpp"

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
	vector<pair<ArrowVariableSizeType, idx_t>> variable_sz_type;
	//! If this is a date/time holds its precision
	vector<ArrowDateTimeType> date_time_precision;
};

struct ArrowProjectedColumns {
	unordered_map<idx_t, string> projection_map;
	vector<string> columns;
};

struct ArrowStreamParameters {
	ArrowProjectedColumns projected_columns;
	TableFilterSet *filters;
};

typedef unique_ptr<ArrowArrayStreamWrapper> (*stream_factory_produce_t)(uintptr_t stream_factory_ptr,
                                                                        ArrowStreamParameters &parameters);
typedef void (*stream_factory_get_schema_t)(uintptr_t stream_factory_ptr, ArrowSchemaWrapper &schema);

struct ArrowScanFunctionData : public PyTableFunctionData {
	ArrowScanFunctionData(stream_factory_produce_t scanner_producer_p, uintptr_t stream_factory_ptr_p)
	    : lines_read(0), stream_factory_ptr(stream_factory_ptr_p), scanner_producer(scanner_producer_p) {
	}
	//! This holds the original list type (col_idx, [ArrowListType,size])
	unordered_map<idx_t, unique_ptr<ArrowConvertData>> arrow_convert_data;
	vector<LogicalType> all_types;
	atomic<idx_t> lines_read;
	ArrowSchemaWrapper schema_root;
	idx_t rows_per_thread;
	//! Pointer to the scanner factory
	uintptr_t stream_factory_ptr;
	//! Pointer to the scanner factory produce
	stream_factory_produce_t scanner_producer;
};

struct ArrowScanLocalState : public LocalTableFunctionState {
	explicit ArrowScanLocalState(unique_ptr<ArrowArrayWrapper> current_chunk) : chunk(std::move(current_chunk)) {
	}

	unique_ptr<ArrowArrayStreamWrapper> stream;
	shared_ptr<ArrowArrayWrapper> chunk;
	idx_t chunk_offset = 0;
	idx_t batch_index = 0;
	vector<column_t> column_ids;
	//! Store child vectors for Arrow Dictionary Vectors (col-idx,vector)
	unordered_map<idx_t, unique_ptr<Vector>> arrow_dictionary_vectors;
	TableFilterSet *filters = nullptr;
	//! The DataChunk containing all read columns (even filter columns that are immediately removed)
	DataChunk all_columns;
};

struct ArrowScanGlobalState : public GlobalTableFunctionState {
	unique_ptr<ArrowArrayStreamWrapper> stream;
	mutex main_mutex;
	idx_t max_threads = 1;
	idx_t batch_index = 0;
	bool done = false;

	vector<idx_t> projection_ids;
	vector<LogicalType> scanned_types;

	idx_t MaxThreads() const override {
		return max_threads;
	}

	bool CanRemoveFilterColumns() const {
		return !projection_ids.empty();
	}
};

struct ArrowTableFunction {
public:
	static void RegisterFunction(BuiltinFunctions &set);

protected:
	//! Binds an arrow table
	static unique_ptr<FunctionData> ArrowScanBind(ClientContext &context, TableFunctionBindInput &input,
	                                              vector<LogicalType> &return_types, vector<string> &names);
	//! Actual conversion from Arrow to DuckDB
	static void ArrowToDuckDB(ArrowScanLocalState &scan_state,
	                          std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data,
	                          DataChunk &output, idx_t start, bool arrow_scan_is_projected = true);

	//! Get next scan state
	static bool ArrowScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
	                                       ArrowScanLocalState &state, ArrowScanGlobalState &parallel_state);

	//! Initialize Global State
	static unique_ptr<GlobalTableFunctionState> ArrowScanInitGlobal(ClientContext &context,
	                                                                TableFunctionInitInput &input);

	//! Initialize Local State
	static unique_ptr<LocalTableFunctionState> ArrowScanInitLocal(ExecutionContext &context,
	                                                              TableFunctionInitInput &input,
	                                                              GlobalTableFunctionState *global_state);

	//! Scan Function
	static void ArrowScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

	//! Defines Maximum Number of Threads
	static idx_t ArrowScanMaxThreads(ClientContext &context, const FunctionData *bind_data);

	//! Allows parallel Create Table / Insertion
	static idx_t ArrowGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
	                                LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state);

	//! -----Utility Functions:-----
	//! Gets Arrow Table's Cardinality
	static unique_ptr<NodeStatistics> ArrowScanCardinality(ClientContext &context, const FunctionData *bind_data);
	//! Gets the progress on the table scan, used for Progress Bars
	static double ArrowProgress(ClientContext &context, const FunctionData *bind_data,
	                            const GlobalTableFunctionState *global_state);
	//! Renames repeated columns and case sensitive columns
	static void RenameArrowColumns(vector<string> &names);
	//! Helper function to get the DuckDB logical type
	static LogicalType GetArrowLogicalType(ArrowSchema &schema,
	                                       std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data,
	                                       idx_t col_idx);
};

} // namespace duckdb
