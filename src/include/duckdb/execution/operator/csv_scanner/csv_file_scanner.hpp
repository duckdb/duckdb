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

namespace duckdb {
struct ReadCSVData;
class CSVFileScan;

struct CSVUnionData {
	~CSVUnionData();

	string file_name;
	vector<string> names;
	vector<LogicalType> types;
	CSVReaderOptions options;
	unique_ptr<CSVFileScan> reader;

	const string &GetFileName() {
		return file_name;
	}
};

//! Struct holding information over a CSV File we will scan
class CSVFileScan {
public:
	using UNION_READER_DATA = unique_ptr<CSVUnionData>;

public:
	//! Constructor for when a CSV File Scan is being constructed over information acquired during sniffing
	//! This means the options are alreadu set, and the buffer manager is already up and runinng.
	CSVFileScan(ClientContext &context, shared_ptr<CSVBufferManager> buffer_manager,
	            shared_ptr<CSVStateMachine> state_machine, const CSVReaderOptions &options,
	            const ReadCSVData &bind_data, const vector<ColumnIndex> &column_ids, CSVSchema &file_schema);
	//! Constructor for new CSV Files, we must initialize the buffer manager and the state machine
	//! Path to this file
	CSVFileScan(ClientContext &context, const string &file_path, const CSVReaderOptions &options, idx_t file_idx,
	            const ReadCSVData &bind_data, const vector<ColumnIndex> &column_ids, CSVSchema &file_schema,
	            bool per_file_single_threaded);

	CSVFileScan(ClientContext &context, const string &file_name, const CSVReaderOptions &options);

public:
	void SetStart();
	void SetNamesAndTypes(const vector<string> &names, const vector<LogicalType> &types);

public:
	const string &GetFileName() const;
	const vector<string> &GetNames();
	const vector<LogicalType> &GetTypes();
	const vector<MultiFileReaderColumnDefinition> &GetColumns();
	void InitializeProjection();
	void Finish();

	static unique_ptr<CSVUnionData> StoreUnionReader(unique_ptr<CSVFileScan> scan_p, idx_t file_idx) {
		auto data = make_uniq<CSVUnionData>();
		if (file_idx == 0) {
			data->file_name = scan_p->file_path;
			data->options = scan_p->options;
			data->names = scan_p->names;
			data->types = scan_p->types;
			data->reader = std::move(scan_p);
		} else {
			data->file_name = scan_p->file_path;
			data->options = std::move(scan_p->options);
			data->names = std::move(scan_p->names);
			data->types = std::move(scan_p->types);
		}
		data->options.auto_detect = false;
		return data;
	}

	//! Initialize the actual names and types to be scanned from the file
	void InitializeFileNamesTypes();

public:
	const string file_path;
	//! File Index
	idx_t file_idx;
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

	MultiFileReaderData reader_data;

	vector<LogicalType> file_types;

	//! Variables to handle projection pushdown
	set<idx_t> projected_columns;
	std::vector<std::pair<idx_t, idx_t>> projection_ids;

	//! Options for this CSV Reader
	CSVReaderOptions options;

	CSVIterator start_iterator;

private:
	vector<string> names;
	vector<LogicalType> types;
	vector<MultiFileReaderColumnDefinition> columns;
};
} // namespace duckdb
