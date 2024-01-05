//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/table_function/global_csv_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/buffer_manager/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/state_machine/csv_state_machine.hpp"
#include "duckdb/execution/operator/csv_scanner/util/csv_error.hpp"
#include "duckdb/function/table/read_csv.hpp"

namespace duckdb {

//! Struct holding information over a CSV File we will scan
class CSVFileScan {
public:
	//! Constructor for when a CSV File Scan is being constructed over information acquired during sniffing
	//! This means the options are alreadu set, and the buffer manager is already up and runinng.
	CSVFileScan(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine,
	            const CSVReaderOptions &options);
	//! Constructor for new CSV Files, we must initialize the buffer manager and the state machine
	//! Path to this file
	CSVFileScan(ClientContext &context, const string &file_path, idx_t file_idx, const CSVReaderOptions &options);
	const string file_path;
	//! File Index
	const idx_t file_idx;
	//! Buffer Manager for the CSV File
	shared_ptr<CSVBufferManager> buffer_manager;
	//! State Machine for this file
	shared_ptr<CSVStateMachine> state_machine;
	//! How many bytes were read up to this point
	atomic<idx_t> bytes_read {0};
	//! Size of this file
	idx_t file_size;
	//! Line Info used in error messages
	shared_ptr<CSVErrorHandler> error_handler;
	//! Whether or not this is an on-disk file
	bool on_disk_file = true;

	//! Options for this CSV Reader
	CSVReaderOptions options;
};

//! CSV Global State is used in the CSV Reader Table Function, it controls what each thread
struct CSVGlobalState : public GlobalTableFunctionState {
public:
	CSVGlobalState(ClientContext &context, shared_ptr<CSVBufferManager> buffer_manager_p,
	               const CSVReaderOptions &options, idx_t system_threads_p, const vector<string> &files,
	               vector<column_t> column_ids_p);

	~CSVGlobalState() override {
	}

	//! Generates a CSV Scanner, with information regarding the piece of buffer it should be read.
	//! In case it returns a nullptr it means we are done reading these files.
	unique_ptr<StringValueScanner> Next(ClientContext &context, const ReadCSVData &bind_data,
	                                    CSVIterator &csv_position);

	void DecrementThread();

	bool Finished();

	//! Returns Current Progress of this CSV Read
	double GetProgress(const ReadCSVData &bind_data) const;

	//! Calculates the Max Threads that will be used by this CSV Reader
	idx_t MaxThreads() const override;
	//! We hold information on the current scanner boundary
	CSVIterator current_boundary;

private:
	//! Reference to the cient context that created this scan
	ClientContext &context;

	vector<unique_ptr<CSVFileScan>> file_scans;

	//! Mutex to lock when getting next batch of bytes (Parallel Only)
	mutex main_mutex;

	//! Basically max number of threads in DuckDB
	idx_t system_threads;

	//! Number of threads being used in this scanner
	idx_t running_threads = 1;
	//! The column ids to read
	vector<column_t> column_ids;

	string sniffer_mismatch_error;

	bool finished = false;
};

} // namespace duckdb
