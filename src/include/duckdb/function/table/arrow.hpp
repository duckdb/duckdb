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
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"

namespace duckdb {

struct ArrowInterval {
	int32_t months;
	int32_t days;
	int64_t nanoseconds;

	inline bool operator==(const ArrowInterval &rhs) const {
		return this->days == rhs.days && this->months == rhs.months && this->nanoseconds == rhs.nanoseconds;
	}
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
public:
	ArrowScanFunctionData(stream_factory_produce_t scanner_producer_p, uintptr_t stream_factory_ptr_p)
	    : lines_read(0), stream_factory_ptr(stream_factory_ptr_p), scanner_producer(scanner_producer_p) {
	}
	vector<LogicalType> all_types;
	atomic<idx_t> lines_read;
	ArrowSchemaWrapper schema_root;
	idx_t rows_per_thread;
	//! Pointer to the scanner factory
	uintptr_t stream_factory_ptr;
	//! Pointer to the scanner factory produce
	stream_factory_produce_t scanner_producer;
	//! Arrow table data
	ArrowTableType arrow_table;
};

struct ArrowScanLocalState : public LocalTableFunctionState {
	explicit ArrowScanLocalState(unique_ptr<ArrowArrayWrapper> current_chunk) : chunk(current_chunk.release()) {
	}

	unique_ptr<ArrowArrayStreamWrapper> stream;
	shared_ptr<ArrowArrayWrapper> chunk;
	// This vector hold the Arrow Vectors owned by DuckDB to allow for zero-copy
	// Note that only DuckDB can release these vectors
	unordered_map<idx_t, shared_ptr<ArrowArrayWrapper>> arrow_owned_data;
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

public:
	//! Binds an arrow table
	static unique_ptr<FunctionData> ArrowScanBind(ClientContext &context, TableFunctionBindInput &input,
	                                              vector<LogicalType> &return_types, vector<string> &names);
	//! Actual conversion from Arrow to DuckDB
	static void ArrowToDuckDB(ArrowScanLocalState &scan_state, const arrow_column_map_t &arrow_convert_data,
	                          DataChunk &output, idx_t start, bool arrow_scan_is_projected = true);

	//! Get next scan state
	static bool ArrowScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
	                                       ArrowScanLocalState &state, ArrowScanGlobalState &parallel_state);

	//! Initialize Global State
	static unique_ptr<GlobalTableFunctionState> ArrowScanInitGlobal(ClientContext &context,
	                                                                TableFunctionInitInput &input);

	//! Initialize Local State
	static unique_ptr<LocalTableFunctionState> ArrowScanInitLocalInternal(ClientContext &context,
	                                                                      TableFunctionInitInput &input,
	                                                                      GlobalTableFunctionState *global_state);
	static unique_ptr<LocalTableFunctionState> ArrowScanInitLocal(ExecutionContext &context,
	                                                              TableFunctionInitInput &input,
	                                                              GlobalTableFunctionState *global_state);

	//! Scan Function
	static void ArrowScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

protected:
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
	static unique_ptr<ArrowType> GetArrowLogicalType(ArrowSchema &schema);
};

} // namespace duckdb
