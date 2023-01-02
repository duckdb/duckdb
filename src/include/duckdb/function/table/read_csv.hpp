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
#include "duckdb/execution/operator/persistent/parallel_csv_reader.hpp"
#include "duckdb/execution/operator/persistent/csv_file_handle.hpp"
#include "duckdb/execution/operator/persistent/csv_buffer.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

class ReadCSV {
public:
	static unique_ptr<CSVFileHandle> OpenCSV(const BufferedCSVReaderOptions &options, ClientContext &context);
};

struct BaseCSVData : public TableFunctionData {
	virtual ~BaseCSVData() {
	}
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
	WriteCSVData(string file_path, vector<LogicalType> sql_types, vector<string> names) : sql_types(std::move(sql_types)) {
		files.push_back(std::move(file_path));
		options.names = std::move(names);
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
	//! The initial reader (if any): this is used when automatic detection is used during binding.
	//! In this case, the CSV reader is already created and might as well be re-used.
	unique_ptr<BufferedCSVReader> initial_reader;
	//! The union readers are created (when csv union_by_name option is on) during binding
	//! Those readers can be re-used during ReadCSVFunction
	vector<unique_ptr<BufferedCSVReader>> union_readers;
	//! Whether or not the single-threaded reader should be used
	bool single_threaded = false;

	void InitializeFiles(ClientContext &context, const vector<string> &patterns);
	void FinalizeRead(ClientContext &context);
};

struct CSVCopyFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ReadCSVTableFunction {
	static TableFunction GetFunction(bool list_parameter = false);
	static TableFunction GetAutoFunction(bool list_parameter = false);
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
