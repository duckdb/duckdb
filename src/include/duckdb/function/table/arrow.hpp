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
	// Map from filter index to column index
	unordered_map<idx_t, idx_t> filter_to_col;
};

struct ArrowStreamParameters {
	ArrowProjectedColumns projected_columns;
	TableFilterSet *filters;
};

typedef unique_ptr<ArrowArrayStreamWrapper> (*stream_factory_produce_t)(uintptr_t stream_factory_ptr,
                                                                        ArrowStreamParameters &parameters);
typedef void (*stream_factory_get_schema_t)(ArrowArrayStream *stream_factory_ptr, ArrowSchema &schema);

struct ArrowScanFunctionData : public TableFunctionData {
public:
	ArrowScanFunctionData(stream_factory_produce_t scanner_producer_p, uintptr_t stream_factory_ptr_p,
	                      shared_ptr<DependencyItem> dependency = nullptr)
	    : lines_read(0), rows_per_thread(0), stream_factory_ptr(stream_factory_ptr_p),
	      scanner_producer(scanner_producer_p), dependency(std::move(dependency)) {
	}

	vector<LogicalType> all_types;
	atomic<idx_t> lines_read;
	ArrowSchemaWrapper schema_root;
	idx_t rows_per_thread;
	//! Pointer to the scanner factory
	uintptr_t stream_factory_ptr;
	//! Pointer to the scanner factory produce
	stream_factory_produce_t scanner_producer;
	//! The (optional) dependency of this function (used in Python for example)
	shared_ptr<DependencyItem> dependency;
	//! Arrow table data
	ArrowTableType arrow_table;
	//! Whether projection pushdown is enabled on the scan
	bool projection_pushdown_enabled = true;
};

struct ArrowRunEndEncodingState {
public:
	ArrowRunEndEncodingState() {
	}

public:
	unique_ptr<Vector> run_ends;
	unique_ptr<Vector> values;

public:
	void Reset() {
		run_ends.reset();
		values.reset();
	}
};

struct ArrowScanLocalState;
struct ArrowArrayScanState {
public:
	explicit ArrowArrayScanState(ArrowScanLocalState &state, ClientContext &context);

public:
	ArrowScanLocalState &state;
	// Hold ownership over the Arrow Arrays owned by DuckDB to allow for zero-copy
	shared_ptr<ArrowArrayWrapper> owned_data;
	unordered_map<idx_t, unique_ptr<ArrowArrayScanState>> children;
	// Optionally holds the pointer that was used to create the cached dictionary
	optional_ptr<ArrowArray> arrow_dictionary = nullptr;
	// Cache the (optional) dictionary of this array
	unique_ptr<Vector> dictionary;
	//! Run-end-encoding state
	ArrowRunEndEncodingState run_end_encoding;
	ClientContext &context;

public:
	ArrowArrayScanState &GetChild(idx_t child_idx);
	void AddDictionary(unique_ptr<Vector> dictionary_p, ArrowArray *arrow_dict);
	bool HasDictionary() const;
	bool CacheOutdated(ArrowArray *dictionary) const;
	Vector &GetDictionary();
	ArrowRunEndEncodingState &RunEndEncoding() {
		return run_end_encoding;
	}

public:
	void Reset() {
		// Note: dictionary is not reset
		// the dictionary should be the same for every array scanned of this column
		run_end_encoding.Reset();
		for (auto &child : children) {
			child.second->Reset();
		}
		owned_data.reset();
	}
};

struct ArrowScanLocalState : public LocalTableFunctionState {
public:
	explicit ArrowScanLocalState(unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &context)
	    : chunk(current_chunk.release()), context(context) {
	}

public:
	unique_ptr<ArrowArrayStreamWrapper> stream;
	shared_ptr<ArrowArrayWrapper> chunk;
	idx_t chunk_offset = 0;
	idx_t batch_index = 0;
	vector<column_t> column_ids;
	unordered_map<idx_t, unique_ptr<ArrowArrayScanState>> array_states;
	TableFilterSet *filters = nullptr;
	//! The DataChunk containing all read columns (even filter columns that are immediately removed)
	DataChunk all_columns;
	ClientContext &context;

public:
	void Reset() {
		chunk_offset = 0;
		for (auto &col : array_states) {
			col.second->Reset();
		}
	}
	ArrowArrayScanState &GetState(idx_t child_idx) {
		auto it = array_states.find(child_idx);
		if (it == array_states.end()) {
			auto child_p = make_uniq<ArrowArrayScanState>(*this, context);
			auto &child = *child_p;
			array_states.emplace(child_idx, std::move(child_p));
			return child;
		}
		return *it->second;
	}
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
	static unique_ptr<FunctionData> ArrowScanBindDumb(ClientContext &context, TableFunctionBindInput &input,
	                                                  vector<LogicalType> &return_types, vector<string> &names);
	//! Actual conversion from Arrow to DuckDB
	static void ArrowToDuckDB(ArrowScanLocalState &scan_state, const arrow_column_map_t &arrow_convert_data,
	                          DataChunk &output, idx_t start, bool arrow_scan_is_projected = true,
	                          idx_t rowid_column_index = COLUMN_IDENTIFIER_ROW_ID);

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
	static void PopulateArrowTableType(DBConfig &config, ArrowTableType &arrow_table,
	                                   const ArrowSchemaWrapper &schema_p, vector<string> &names,
	                                   vector<LogicalType> &return_types);

protected:
	//! Defines Maximum Number of Threads
	static idx_t ArrowScanMaxThreads(ClientContext &context, const FunctionData *bind_data);

	//! Allows parallel Create Table / Insertion
	static OperatorPartitionData ArrowGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input);

	//! Specify if a given type can be pushed-down by the arrow engine
	static bool ArrowPushdownType(const LogicalType &type);
	//! -----Utility Functions:-----
	//! Gets Arrow Table's Cardinality
	static unique_ptr<NodeStatistics> ArrowScanCardinality(ClientContext &context, const FunctionData *bind_data);
	//! Gets the progress on the table scan, used for Progress Bars
	static double ArrowProgress(ClientContext &context, const FunctionData *bind_data,
	                            const GlobalTableFunctionState *global_state);
};

} // namespace duckdb
