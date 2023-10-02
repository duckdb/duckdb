//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/read_csv.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/scan/csv/buffered_csv_reader.hpp"
#include "duckdb/execution/operator/scan/csv/csv_buffer.hpp"
#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_file_handle.hpp"
#include "duckdb/execution/operator/scan/csv/csv_reader_options.hpp"
#include "duckdb/execution/operator/scan/csv/parallel_csv_reader.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine_cache.hpp"

namespace duckdb {

class ReadCSV {
public:
	static unique_ptr<CSVFileHandle> OpenCSV(const string &file_path, FileCompressionType compression,
	                                         ClientContext &context);
};

struct BaseCSVData : public TableFunctionData {
	virtual ~BaseCSVData() {
	}
	//! The file path of the CSV file to read or write
	vector<string> files;
	//! The CSV reader options
	CSVReaderOptions options;
	//! Offsets for generated columns
	idx_t filename_col_idx;
	idx_t hive_partition_col_idx;

	void Finalize();
};

struct WriteCSVData : public BaseCSVData {
	WriteCSVData(string file_path, vector<LogicalType> sql_types, vector<string> names)
	    : sql_types(std::move(sql_types)) {
		files.push_back(std::move(file_path));
		options.name_list = std::move(names);
	}

	//! The SQL types to write
	vector<LogicalType> sql_types;
	//! The newline string to write
	string newline = "\n";
	//! The size of the CSV file (in bytes) that we buffer before we flush it to disk
	idx_t flush_size = 4096 * 8;
	//! For each byte whether or not the CSV file requires quotes when containing the byte
	unsafe_unique_array<bool> requires_quotes;
};

struct ColumnInfo {
	ColumnInfo() {
	}
	ColumnInfo(vector<std::string> names_p, vector<LogicalType> types_p) {
		names = std::move(names_p);
		types = std::move(types_p);
	}
	void Serialize(Serializer &serializer) const;
	static ColumnInfo Deserialize(Deserializer &deserializer);

	vector<std::string> names;
	vector<LogicalType> types;
};

struct ReadCSVData : public BaseCSVData {
	//! The expected SQL types to read from the file
	vector<LogicalType> csv_types;
	//! The expected SQL names to be read from the file
	vector<string> csv_names;
	//! The expected SQL types to be returned from the read - including added constants (e.g. filename, hive partitions)
	vector<LogicalType> return_types;
	//! The expected SQL names to be returned from the read - including added constants (e.g. filename, hive partitions)
	vector<string> return_names;
	//! The buffer manager (if any): this is used when automatic detection is used during binding.
	//! In this case, some CSV buffers have already been read and can be reused.
	shared_ptr<CSVBufferManager> buffer_manager;
	unique_ptr<BufferedCSVReader> initial_reader;
	//! The union readers are created (when csv union_by_name option is on) during binding
	//! Those readers can be re-used during ReadCSVFunction
	vector<unique_ptr<BufferedCSVReader>> union_readers;
	//! Whether or not the single-threaded reader should be used
	bool single_threaded = false;
	//! Reader bind data
	MultiFileReaderBindData reader_bind;
	vector<ColumnInfo> column_info;
	//! The CSVStateMachineCache caches state machines created for sniffing and parsing csv files
	//! We cache them because when reading very small csv files, the cost of creating all the possible
	//! State machines for sniffing becomes a major bottleneck.
	CSVStateMachineCache state_machine_cache;

	void Initialize(unique_ptr<BufferedCSVReader> &reader) {
		this->initial_reader = std::move(reader);
	}
	void FinalizeRead(ClientContext &context);

	void Serialize(Serializer &serializer) const;
	static unique_ptr<ReadCSVData> Deserialize(Deserializer &deserializer);
};

struct CSVCopyFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ReadCSVTableFunction {
	static TableFunction GetFunction();
	static TableFunction GetAutoFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
