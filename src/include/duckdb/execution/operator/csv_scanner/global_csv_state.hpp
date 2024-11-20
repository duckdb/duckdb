//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/global_csv_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_state_machine.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_error.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_file_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/string_value_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_validator.hpp"

namespace duckdb {

//! CSV Global State is used in the CSV Reader Table Function, it controls what each thread
struct CSVGlobalState : public GlobalTableFunctionState {
	CSVGlobalState(ClientContext &context, const shared_ptr<CSVBufferManager> &buffer_manager_p,
	               const CSVReaderOptions &options, idx_t system_threads_p, const vector<string> &files,
	               vector<ColumnIndex> column_ids_p, const ReadCSVData &bind_data);

	~CSVGlobalState() override {
	}

	//! Generates a CSV Scanner, with information regarding the piece of buffer it should be read.
	//! In case it returns a nullptr it means we are done reading these files.
	unique_ptr<StringValueScanner> Next(optional_ptr<StringValueScanner> previous_scanner);

	void FillRejectsTable();

	void DecrementThread();

	//! Returns Current Progress of this CSV Read
	double GetProgress(const ReadCSVData &bind_data) const;

	//! Calculates the Max Threads that will be used by this CSV Reader
	idx_t MaxThreads() const override;

	bool IsDone() const;

private:
	//! Reference to the client context that created this scan
	ClientContext &context;

	vector<shared_ptr<CSVFileScan>> file_scans;

	//! Mutex to lock when getting next batch of bytes (Parallel Only)
	mutable mutex main_mutex;

	//! Basically max number of threads in DuckDB
	idx_t system_threads;

	//! Number of threads being used in this scanner
	idx_t running_threads = 1;
	//! The column ids to read
	vector<ColumnIndex> column_ids;

	string sniffer_mismatch_error;

	bool finished = false;

	const ReadCSVData &bind_data;

	CSVSchema file_schema;

	bool single_threaded = false;

	atomic<idx_t> scanner_idx;

	atomic<idx_t> last_file_idx;
	shared_ptr<CSVBufferUsage> current_buffer_in_use;

	unordered_map<idx_t, idx_t> threads_per_file;
	//! We hold information on the current scanner boundary
	CSVIterator current_boundary;

	CSVValidator validator;
};

} // namespace duckdb
