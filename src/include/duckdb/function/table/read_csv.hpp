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
#include "duckdb/execution/operator/persistent/csv_buffer.hpp"

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
public:
	ReadCSVGlobalState(unique_ptr<CSVFileHandle> file_handle_p, vector<string> &files_path_p, idx_t system_threads_p,
	                   idx_t buffer_size_p)
	    : file_handle(move(file_handle_p)), system_threads(system_threads_p), buffer_size(buffer_size_p) {
		file_size = file_handle->FileSize();
		first_file_size = file_size;
		bytes_per_local_state = buffer_size / MaxThreads();
		current_buffer = make_shared<CSVBuffer>(buffer_size, *file_handle);
		next_buffer = current_buffer->Next(*file_handle);
	}

	ReadCSVGlobalState() {};

	idx_t MaxThreads() const override;
	//! Returns buffer and index that caller thread should read.
	CSVBufferRead Next(ClientContext &context, ReadCSVData &bind_data);
	//! If we finished reading all the CSV Files
	bool Finished();

private:
	//! File Handle for current file
	unique_ptr<CSVFileHandle> file_handle;

	shared_ptr<CSVBuffer> current_buffer;
	shared_ptr<CSVBuffer> next_buffer;
	//! The index of the next file to read (i.e. current file + 1)
	idx_t file_index = 1;
	//! How many bytes were read up to this point
	//	atomic<idx_t> bytes_read;

	//! Mutex to lock when getting next batch of bytes (Parallel Only)
	mutex main_mutex;
	//! Byte set from for last thread
	idx_t next_byte = 0;
	//! Size of current file
	idx_t file_size;

	//! How many bytes we should execute per local state
	idx_t bytes_per_local_state;

	//! Size of first file
	idx_t first_file_size;
	//! Basically max number of threads in DuckDB
	idx_t system_threads;
	//! Size of the buffers
	idx_t buffer_size;
};

struct ReadCSVLocalState : public LocalTableFunctionState {
public:
	explicit ReadCSVLocalState(unique_ptr<BufferedCSVReader> csv_reader_p) : csv_reader(move(csv_reader_p)) {
	}
	//! The CSV reader
	unique_ptr<BufferedCSVReader> csv_reader;
};

} // namespace duckdb
