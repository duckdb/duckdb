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
#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"

namespace duckdb {

struct BaseCSVData : public TableFunctionData {
	//! The file path of the CSV file to read or write
	vector<string> files;
	//! The CSV reader options
	BufferedCSVReaderOptions options;

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
	//! The initial reader (if any): this is used when automatic detection is used during binding.
	//! In this case, the CSV reader is already created and might as well be re-used.
	unique_ptr<BufferedCSVReader> initial_reader;
};

struct CSVCopyFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ReadCSVTableFunction {
	static TableFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
