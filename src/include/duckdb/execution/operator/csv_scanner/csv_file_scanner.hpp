//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/csv_file_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_state_machine.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_error.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_schema.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_validator.hpp"
#include "duckdb/common/base_file_reader.hpp"

namespace duckdb {
struct ReadCSVData;
class CSVFileScan;

struct CSVUnionData : public BaseUnionData {
	explicit CSVUnionData(string file_name_p) : BaseUnionData(std::move(file_name_p)) {
	}
	~CSVUnionData() override;

	CSVReaderOptions options;
};

//! Struct holding information over a CSV File we will scan
class CSVFileScan : public BaseFileReader {
public:
	//! Constructor for new CSV Files, we must initialize the buffer manager and the state machine
	//! Path to this file
	CSVFileScan(ClientContext &context, const string &file_path, CSVReaderOptions options,
	            const MultiFileReaderOptions &file_options, const vector<string> &names,
	            const vector<LogicalType> &types, CSVSchema &file_schema, bool per_file_single_threaded,
	            shared_ptr<CSVBufferManager> buffer_manager = nullptr, bool fixed_schema = false);

	CSVFileScan(ClientContext &context, const string &file_name, const CSVReaderOptions &options,
	            const MultiFileReaderOptions &file_options);

public:
	void SetStart();
	void SetNamesAndTypes(const vector<string> &names, const vector<LogicalType> &types);

public:
	idx_t GetFileIndex() const {
		return reader_data.file_list_idx.GetIndex();
	}
	const vector<string> &GetNames();
	const vector<LogicalType> &GetTypes();
	void InitializeProjection();
	void Finish();

	//! Initialize the actual names and types to be scanned from the file
	void InitializeFileNamesTypes();

public:
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

	vector<LogicalType> file_types;

	//! Variables to handle projection pushdown
	set<idx_t> projected_columns;
	std::vector<std::pair<idx_t, idx_t>> projection_ids;

	//! Options for this CSV Reader
	CSVReaderOptions options;

	CSVIterator start_iterator;

	CSVValidator validator;

	//! The started tasks and finished tasks allow us to track if all reads of the CSV file have completed
	//! Note that the "started_tasks" starts at one - this is so we can track when the scheduling of all tasks for this
	//! file has completed When the scheduling is finished we increment `finished_tasks` by one as well
	atomic<idx_t> started_tasks {1};
	atomic<idx_t> finished_tasks {0};

private:
	vector<string> names;
	vector<LogicalType> types;
};
} // namespace duckdb
