//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/table_function/global_csv_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_line_info.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine.hpp"
#include "duckdb/execution/operator/scan/csv/scanner/scanner_boundary.hpp"
#include "duckdb/function/table/read_csv.hpp"

namespace duckdb {

//! CSV Global State is used in the CSV Reader Table Function, it controls what each thread
struct CSVGlobalState : public GlobalTableFunctionState {
public:
	CSVGlobalState(ClientContext &context, shared_ptr<CSVBufferManager> buffer_manager_p,
	               const CSVReaderOptions &options, idx_t system_threads_p, const vector<string> &files,
	               vector<column_t> column_ids_p, const StateMachine &state_machine_p);

	~CSVGlobalState() override {
	}

	//! How many bytes were read up to this point
	atomic<idx_t> bytes_read {0};

	//! Size of the first file
	//! TODO: we now have an assumption that all files have similar sizes, we possibly should do more fine-grained
	//! Optimizations to multi-file readers.
	idx_t file_size;

	shared_ptr<CSVStateMachine> state_machine;

	//! Generates a CSV Scanner, with information regarding the piece of buffer it should be read.
	//! In case it returns a nullptr it means we are done reading these files.
	unique_ptr<StringValueScanner> Next(ClientContext &context, const ReadCSVData &bind_data);
	//! Verify if the CSV File was read correctly
	void Verify();

	//	void UpdateVerification(VerificationPositions positions, idx_t file_number, idx_t batch_idx);

	//	void UpdateLinesRead(CSVScanner &buffer_read, idx_t file_idx);

	void DecrementThread();

	bool Finished();

	//! Returns Current Progress of this CSV Read
	double GetProgress(const ReadCSVData &bind_data) const;

	//! Calculates the Max Threads that will be used by this CSV Reader
	idx_t MaxThreads() const override;

private:
	//! Buffer Manager for the CSV Files in this Scan
	shared_ptr<CSVBufferManager> buffer_manager;
	//! We hold information on the current scanner boundary
	ScannerBoundary current_boundary;

	//! Mutex to lock when getting next batch of bytes (Parallel Only)
	mutex main_mutex;

	//! Whether or not this is an on-disk file
	bool on_disk_file = true;

	//! Basically max number of threads in DuckDB
	idx_t system_threads;

	//! Current File Number
	idx_t max_tuple_end = 0;

	//! The vector stores positions where threads ended the last line they read in the CSV File, and the set stores
	//! Positions where they started reading the first line.
	vector<vector<idx_t>> tuple_end;
	vector<set<idx_t>> tuple_start;
	//! Tuple end to batch
	vector<unordered_map<idx_t, idx_t>> tuple_end_to_batch;
	//! Batch to Tuple End
	vector<unordered_map<idx_t, idx_t>> batch_to_tuple_end;
	//! Number of threads being used in this scanner
	idx_t running_threads = 1;
	//! The column ids to read
	vector<column_t> column_ids;
	//! Line Info used in error messages
	LineInfo line_info;

	CSVStateMachineCache cache;
	string sniffer_mismatch_error;

	bool finished = false;

	//! Initilizes verification variables
	void InitializeVerificationVariables(const CSVReaderOptions &options, idx_t file_count);
};

} // namespace duckdb
