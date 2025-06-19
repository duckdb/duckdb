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
struct MultiFileBindData;

//! CSV Global State is used in the CSV Reader Table Function, it controls what each thread
struct CSVGlobalState : public GlobalTableFunctionState {
public:
	CSVGlobalState(ClientContext &context_p, const CSVReaderOptions &options, idx_t total_file_count,
	               const MultiFileBindData &bind_data);

	~CSVGlobalState() override {
	}

	//! Generates a CSV Scanner, with information regarding the piece of buffer it should be read.
	//! In case it returns a nullptr it means we are done reading these files.
	void FinishScan(unique_ptr<StringValueScanner> scanner);
	unique_ptr<StringValueScanner> Next(shared_ptr<CSVFileScan> &file);
	void FinishLaunchingTasks(CSVFileScan &scan);

	void FillRejectsTable(CSVFileScan &scan);
	void FinishTask(CSVFileScan &scan);
	void FinishFile(CSVFileScan &scan);

	//! Whether or not to read individual CSV files single-threaded
	bool SingleThreadedRead() const {
		return single_threaded;
	}

private:
	//! Reference to the client context that created this scan
	ClientContext &context;
	const MultiFileBindData &bind_data;

	string sniffer_mismatch_error;

	bool initialized = false;

	bool single_threaded = false;

	atomic<idx_t> scanner_idx;

	shared_ptr<CSVBufferUsage> current_buffer_in_use;

	//! We hold information on the current scanner boundary
	CSVIterator current_boundary;

	vector<idx_t> rejects_file_indexes;
};

} // namespace duckdb
