//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/read_csv.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/scalar/strftime.hpp"
#include "duckdb/execution/operator/persistent/csv_reader_options.hpp"
#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"
#include "duckdb/execution/operator/persistent/csv_file_handle.hpp"

namespace duckdb {

class ReadCSV {
public:
	static unique_ptr<CSVFileHandle> OpenCSV(const BufferedCSVReaderOptions &options, ClientContext &context);
};

struct BaseCSVData : public TableFunctionData {
	//! The file path of the CSV file to read or write
	vector<string> files;
	//! The CSV reader options
	BufferedCSVReaderOptions options;
	//! Offsets for generated columns
	idx_t filename_col_idx;
	idx_t hive_partition_col_idx;

	void Finalize();
};

struct WriteCSVData : public BaseCSVData {
	WriteCSVData(string file_path, vector<LogicalType> sql_types, vector<string> names) : sql_types(move(sql_types)) {
		files.push_back(move(file_path));
		options.names = move(names);
	}

	//! The SQL types to write
	vector<LogicalType> sql_types;
	//! The newline string to write
	string newline = "\n";
	//! Whether or not we are writing a simple CSV (delimiter, quote and escape are all 1 byte in length)
	bool is_simple;
	//! The size of the CSV file (in bytes) that we buffer before we flush it to disk
	idx_t flush_size = 4096 * 8;
};

struct ReadCSVData : public BaseCSVData {
	//! The expected SQL types to read
	vector<LogicalType> sql_types;
	//! The initial file handler (if any): used in combination with the initial reader for automatic detection.
	unique_ptr<CSVFileHandle> initial_file_handler;
	//! The initial reader (if any): this is used when automatic detection is used during binding.
	//! In this case, the CSV reader is already created and might as well be re-used.
	unique_ptr<BufferedCSVReader> initial_reader;
	//! The union readers is created(when csv union_by_name option is on) during binding
	//! Those readers can be re-used during ReadCSVFunction
	vector<unique_ptr<CSVFileHandle>> union_file_handlers;
	vector<unique_ptr<BufferedCSVReader>> union_readers;
};

struct CSVCopyFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ReadCSVTableFunction {
	static TableFunction GetFunction(bool list_parameter = false);
	static TableFunction GetAutoFunction(bool list_parameter = false);
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ReadCSVGlobalState : public GlobalTableFunctionState {
	//! File Handle for current file
	unique_ptr<CSVFileHandle> file_handle;
	//! The index of the next file to read (i.e. current file + 1)
	idx_t file_index;
	//! How many bytes were read up to this point
	atomic<idx_t> bytes_read;
	//! Mutex to lock when getting next batch of bytes (Parallel Only)
	mutex main_mutex;
	//! Starting Byte of Next Thread
	idx_t next_byte;
	//! Size of current file
	idx_t file_size;
	//! How many bytes we should execute per local state
	idx_t bytes_per_local_state;
	//! If we finished reading the whole file
	bool finished_file = false;

	idx_t children = 0;
	idx_t children_done = 0;

	idx_t MaxThreads() const override;
};

struct ReadCSVLocalState : public LocalTableFunctionState {
	ReadCSVLocalState(ReadCSVGlobalState &global_state, idx_t bytes_per_thread_p,
	                  unique_ptr<BufferedCSVReader> csv_reader_p)
	    : global_state(global_state), bytes_per_thread(bytes_per_thread_p), csv_reader(move(csv_reader_p)) {
		file_size = csv_reader->file_handle->FileSize();
		csv_reader->bytes_per_thread = bytes_per_thread;
		Next();
	}

	void Next() {
		if (start_byte > file_size) {
			return;
		}
		{
			lock_guard<mutex> parallel_lock(global_state.main_mutex);
			// Update the Global State with Remainder Lines
			if (csv_reader->reminder_start) {
				auto start = csv_reader->reminder_start->start;
				global_state.remainder_lines.start_remainder[start] = move(csv_reader->reminder_start);
			}
			if (csv_reader->reminder_end) {
				auto start = csv_reader->reminder_end->start;
				global_state.remainder_lines.end_remainder[start] = move(csv_reader->reminder_end);
			}
			// Update Global State Next Position
			start_byte = global_state.next_byte;
			global_state.next_byte += bytes_per_thread;
			if (start_byte > file_size) {
				global_state.finished_file = true;
				global_state.children_done++;
			}
		}
		// We have to set the positions of the csv_reader
		csv_reader->ResetBuffer();
		csv_reader->start_buffer = start_byte;
		csv_reader->position_buffer = start_byte;
		csv_reader->start_position_csv = start_byte;
		csv_reader->start_reading = false;
		csv_reader->finished = false;
	}

	bool Valid() {
		return start_byte < file_size;
	}

	ReadCSVGlobalState &global_state;
	//! Start Byte (Relative to CSV File)
	idx_t start_byte = 0;
	//! How many bytes per thread
	idx_t bytes_per_thread = 0;
	//! The File Size
	idx_t file_size = 0;
	//! The CSV reader
	unique_ptr<BufferedCSVReader> csv_reader;
	unique_ptr<CSVFileHandle> file_handle;
};

} // namespace duckdb
