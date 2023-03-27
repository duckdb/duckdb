//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/multi_file_reader_options.hpp"

namespace duckdb {
class TableFunction;
class ClientContext;
class Value;


struct MultiFileReader {
	//! Add the parameters for multi-file readers (e.g. union_by_name, filename) to a table function
	DUCKDB_API static void AddParameters(TableFunction &table_function);
	//! Performs any globbing for the multi-file reader and returns a list of files to be read
	DUCKDB_API static vector<string> GetFileList(ClientContext &context, const Value &input, const string &name);
	//! Parse the named parameters of a multi-file reader for a COPY statement
	DUCKDB_API static bool ParseCopyOption(const string &key, const vector<Value> &values, MultiFileReaderOptions &options);
	//! Parse the named parameters of a multi-file reader
	DUCKDB_API static bool ParseOption(const string &key, const Value &val, MultiFileReaderOptions &options);
	//! Perform complex filter pushdown into the multi-file reader, potentially filtering out files that should be read
	//! If "true" the first file has been eliminated
	DUCKDB_API static bool ComplexFilterPushdown(ClientContext &context, vector<string> &files, const MultiFileReaderOptions &options, LogicalGet &get, vector<unique_ptr<Expression>> &filters);
};

} // namespace duckdb
