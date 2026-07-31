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

//! Local state of the CSV scan
struct CSVLocalState : public LocalTableFunctionState {
	//! Claims that die without ever being scanned still account their boundary lines
	~CSVLocalState() override;

	//! Constructs the scanner from the pending claim
	void Materialize();

	//! Lifecycle of the claim held by this local state
	enum class ClaimState : uint8_t {
		IDLE,        //! no claim held
		PENDING,     //! claim fields are set, the scanner is not constructed yet
		MATERIALIZED //! the scanner was constructed from the claim and owns its accounting
	};

	ClaimState claim_state = ClaimState::IDLE;
	idx_t scanner_idx = 0;
	CSVIterator iterator;
	shared_ptr<CSVBufferUsage> buffer_tracker;
	shared_ptr<CSVFileScan> file_scan;
	unique_ptr<StringValueScanner> csv_reader;
};

//! CSV Global State is used in the CSV Reader Table Function, it controls what each thread
struct CSVGlobalState : public GlobalTableFunctionState {
public:
	CSVGlobalState(ClientContext &context_p, const CSVReaderOptions &options, idx_t total_file_count,
	               const MultiFileBindData &bind_data);

	~CSVGlobalState() override {
	}

	void FinishScan(unique_ptr<StringValueScanner> scanner);
	//! Claims the next piece of the current file to be read into the local state.
	//! Returns false when we are done reading these files.
	bool Next(shared_ptr<CSVFileScan> &file, CSVLocalState &lstate);
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
